from __future__ import annotations

import json
import os
from datetime import timedelta
from typing import Optional

import duckdb
import pendulum
import requests
from airflow import DAG
from airflow.datasets import Dataset
from airflow.datasets.metadata import Metadata
from airflow.decorators import task
from pydantic import BaseModel, TypeAdapter


class APIMetadata(BaseModel):
    total: int
    page: int
    per_page: int


class BreweryData(BaseModel):
    id: str
    name: str
    brewery_type: str
    city: str
    state_province: str
    postal_code: str
    country: str
    state: str
    address_1: Optional[str]
    address_2: Optional[str]
    address_3: Optional[str]
    longitude: Optional[float]
    latitude: Optional[float]
    phone: Optional[str]
    website_url: Optional[str]
    street: Optional[str]


def get_api_metadata() -> APIMetadata:
    """Fetches metadata from the openbrewerydb API and converts to a pydantic
    model.

    Returns:
        APIMetadata: Information regarding the number of entries.
    """
    url = "https://api.openbrewerydb.org/v1/breweries/meta"
    return APIMetadata(**requests.request("GET", url).json())


def get_api_data(page: int, per_page: int = 50) -> list[BreweryData]:
    """Fetches data from the openbrewerydb API and converts to a pydantic
    model.

    Args:
        page(int): Page to get data.
        per_page(int): Number of entries per page. Defaults to `50`.

    Returns:
        list[BreweryData]: List of entries from the API.
    """
    url = "https://api.openbrewerydb.org/v1/breweries"
    response = requests.request("GET", url, params={"per_page": per_page, "page": page})
    return TypeAdapter(list[BreweryData]).validate_python(response.json())


def save_json(filepath: str, data: list[dict] | dict):
    """Save a json to a given filepath.

    Args:
        filepath(str): path to save the file.
        data(list[dict] | dict): data to save to a JSON file.
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w") as file:
        file.write(json.dumps(data))


def generate_aggregate_query(
    read_filepath: str, read_method: str = "read_parquet"
) -> str:
    """Query to create the aggregation of the quantity
    of brewery types and location using DuckDB.

    Args:
        read_filepath(str): file path for reading the data.
        read_method(str): Method to read the file. Defaults to `read_parquet`.

    Returns:
        str: generated query.
    """
    return f"""
        SELECT 
            brewery_type
            , country
            , state
            , city
            , COUNT(*)
        FROM {read_method}('{read_filepath}')
        GROUP BY
            brewery_type
            , country
            , state
            , city
        """


def generate_transform_query(
    read_filepath: str, read_method: str = "read_json_auto"
) -> str:
    """Query to transform JSON data using DuckDB.

    Args:
        read_filepath(str): file path for reading the data.
        read_method(str): Method to read the file. Defaults to `read_json_auto`.

    Returns:
        str: generated query.
    """
    return f"""
        SELECT *
        FROM {read_method}('{read_filepath}')
        """


with DAG(
    dag_id="brewery_dataset",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule_interval="0 0 * * *",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    catchup=False,
    tags=["producer", "dataset", "brewery", "analytics"],
):
    raw_dataset = Dataset("/opt/airflow/dags/files/bronze/breweries.json")
    silver_dataset = Dataset("/opt/airflow/dags/files/silver/breweries")
    gold_dataset = Dataset(
        "/opt/airflow/dags/files/gold/aggregate_breweries_count.parquet"
    )

    @task(outlets=[raw_dataset])
    def save_bronze():
        metadata = get_api_metadata()

        data = []

        for page in range(1, metadata.total // metadata.per_page + 2):
            data.extend(get_api_data(page=page, per_page=metadata.per_page))

        assert len(data) == metadata.total, "Unable do fetch all data"

        save_json(raw_dataset.uri, [entry.model_dump() for entry in data])
        yield Metadata(raw_dataset, {"row_count": len(data)})

    @task(inlets=[raw_dataset], outlets=[silver_dataset])
    def save_silver():
        os.makedirs(os.path.dirname(silver_dataset.uri), exist_ok=True)
        con = duckdb.connect(database=":memory:")

        transform_query = generate_transform_query(raw_dataset.uri)
        partition_query = f"""
            COPY
            (
                {transform_query}
            ) TO '{silver_dataset.uri}'
            (
                FORMAT PARQUET,
                PARTITION_BY (country, state, city),
                OVERWRITE_OR_IGNORE
            );
            """

        con.execute(partition_query)

        con.close()

    @task(inlets=[silver_dataset], outlets=[gold_dataset])
    def save_gold():
        os.makedirs(os.path.dirname(gold_dataset.uri), exist_ok=True)
        con = duckdb.connect(database=":memory:")
        aggregate_query = generate_aggregate_query(
            read_filepath=f"{silver_dataset.uri}/**/*.parquet"
        )

        con.execute(
            f"COPY ({aggregate_query}) TO '{gold_dataset.uri}' (FORMAT PARQUET);"
        )

        con.close()

    save_bronze() >> save_silver() >> save_gold()  # type: ignore
