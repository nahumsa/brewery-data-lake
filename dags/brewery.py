from __future__ import annotations

import json
import os
from datetime import timedelta
from typing import Any, Optional

import duckdb
import pendulum
import requests
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.exceptions import AirflowException
from pydantic import BaseModel, TypeAdapter, ValidationError
from sqlalchemy.log import logging

from airflow import DAG


class APIMetadata(BaseModel):
    total: int
    page: int
    per_page: int


class BreweryData(BaseModel):
    id: str
    name: str
    brewery_type: str
    state_province: str
    postal_code: str
    country: str
    state: str
    city: str
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
        ORDER BY
            brewery_type DESC
            , country DESC
            , state DESC
            , city DESC
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
        SELECT
            id
            , name
            , address_1
            , address_2
            , address_3
            , LOWER(brewery_type) AS brewery_type
            , LOWER(city) AS city
            , LOWER(state_province) AS state_province
            , LOWER(state) AS state
            , LOWER(country) AS contry
            , postal_code
            , CAST(longitude AS FLOAT) AS longitude
            , CAST(latitude AS FLOAT) AS latitude
            , phone
            , CASE
                WHEN website_url LIKE '%@gmail.com%' THEN NULL
                ELSE website_url
            END AS website_url
        FROM {read_method}('{read_filepath}')
        """


def sla_callback(
    dag: Any,
    task_list: list[Any],
    blocking_task_list: list[Any],
    slas: Any,
    blocking_tis: Any,
):
    """Callback for when a SLA is missed, this could
    be an email or slack message.

    Args:
        dag: DAG that missed the SLA.
        task_list: list of tasks in the dag.
        blocking_task_list: list of tasks that missed.
        slas: which slas missed
        blocking_tis: ...
    """
    print(
        "Missed the SLAs in the following DAG",
        {
            "dag": dag,
            "task_list": task_list,
            "blocking_task_list": blocking_task_list,
            "slas": slas,
            "blocking_tis": blocking_tis,
        },
    )


with DAG(
    dag_id="brewery_dataset",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule_interval="0 0 * * *",
    default_args={
        "owner": "Nahum",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    catchup=False,
    tags=["producer", "dataset", "brewery", "analytics"],
    sla_miss_callback=sla_callback,  # type: ignore
):
    raw_dataset = Dataset("/opt/airflow/dags/files/bronze/breweries.json")
    silver_dataset = Dataset("/opt/airflow/dags/files/silver/breweries")
    gold_dataset = Dataset(
        "/opt/airflow/dags/files/gold/aggregate_breweries_count.parquet"
    )

    @task(outlets=[raw_dataset], sla=timedelta(minutes=5))
    def save_bronze(*, outlet_events: Any) -> bool:
        metadata = get_api_metadata()

        data = []

        try:
            for page in range(1, metadata.total // metadata.per_page + 2):
                data.extend(get_api_data(page=page, per_page=metadata.per_page))

        except ValidationError as e:
            logging.error(f"Validation error for the API data model: {e}")
            raise AirflowException(f"Validation error for the API data model: {e}")

        save_json(raw_dataset.uri, [entry.model_dump() for entry in data])

        outlet_events[raw_dataset].extra = {"row_count": len(data)}

        return len(data) == metadata.total

    @task.short_circuit()
    def bronze_evaluator(value: bool) -> bool:
        return value

    @task(
        inlets=[raw_dataset],
        outlets=[silver_dataset],
        sla=timedelta(minutes=1),
    )
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

    @task(
        inlets=[silver_dataset],
        outlets=[gold_dataset],
        sla=timedelta(minutes=1),
    )
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

    bronze_evaluator(save_bronze()) >> save_silver() >> save_gold()  # type: ignore
