from __future__ import annotations
import json
import os

import pendulum
import requests
import duckdb

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task

with DAG(
    dag_id="brewery",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=["producer", "dataset"],
):
    raw_dataset = Dataset("/opt/airflow/dags/files/bronze/breweries.json")
    silver_dataset = Dataset("/opt/airflow/dags/files/silver/breweries")
    gold_dataset = Dataset(
        "/opt/airflow/dags/files/gold/aggregate_breweries_count.parquet"
    )

    @task(outlets=[raw_dataset])
    def fetch_api():
        os.makedirs(os.path.dirname(raw_dataset.uri), exist_ok=True)

        url = "https://api.openbrewerydb.org/v1/breweries/meta"
        metadata = requests.request("GET", url).json()

        data = []

        for page in range(int(metadata["total"]) // int(metadata["per_page"]) + 1):
            url = "https://api.openbrewerydb.org/v1/breweries?per_page=50"
            response = requests.request(
                "GET", url, params={"per_page": 50, "page": page}
            )
            data.extend(response.json())

        with open(raw_dataset.uri, "w") as file:
            file.write(json.dumps(data))

    @task(inlets=[raw_dataset], outlets=[silver_dataset])
    def save_silver(*, inlet_events):
        os.makedirs(os.path.dirname(silver_dataset.uri), exist_ok=True)
        con = duckdb.connect(database=":memory:")
        events = inlet_events[raw_dataset]
        for e in events:
            print(e)

        con.execute(
            f"""
            COPY 
            (
                SELECT * 
                FROM read_json_auto('{raw_dataset.uri}')
            ) TO '{silver_dataset.uri}' 
            (
                FORMAT PARQUET,
                PARTITION_BY (country, state, city),
                OVERWRITE_OR_IGNORE
            );
            """
        )

        con.close()

    @task(inlets=[silver_dataset], outlets=[gold_dataset])
    def save_gold():
        os.makedirs(os.path.dirname(gold_dataset.uri), exist_ok=True)
        con = duckdb.connect(database=":memory:")
        aggregate_query = f"""
        SELECT 
            brewery_type
            , country
            , state
            , city
            , COUNT(*)
        FROM read_parquet('{silver_dataset.uri}/**/*.parquet')
        GROUP BY 
            brewery_type
            , country
            , state
            , city
        """
        con.execute(aggregate_query)

        con.execute(
            f"COPY ({aggregate_query}) TO '{gold_dataset.uri}' (FORMAT PARQUET);"
        )

        con.close()

    fetch_api() >> save_silver() >> save_gold()
