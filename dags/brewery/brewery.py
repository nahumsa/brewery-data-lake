from __future__ import annotations

import json
import os
from datetime import timedelta
from typing import Any

import duckdb
import pendulum
from airflow.datasets import Dataset
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowException
from brewery.duckdb.aggregate import generate_aggregate_query
from brewery.duckdb.data_quality import (
    generate_id_uniquiness_query,
    generate_valid_brewery_type_query,
)
from brewery.duckdb.transform import generate_transform_query
from brewery.extract.api import get_api_data, get_api_metadata
from pydantic import ValidationError
from sqlalchemy.log import logging

from airflow import DAG


def save_json(filepath: str, data: list[dict] | dict):
    """Save a json to a given filepath.

    Args:
        filepath(str): path to save the file.
        data(list[dict] | dict): data to save to a JSON file.
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w") as file:
        file.write(json.dumps(data))


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
    base_path = "/opt/airflow/data/brewery/"

    raw_dataset = Dataset(f"{base_path}bronze/breweries.json")
    silver_dataset = Dataset(f"{base_path}silver/breweries")
    gold_dataset = Dataset(f"{base_path}gold/aggregate_breweries_count.parquet")

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

    @task_group
    def data_quality_check():
        @task.short_circuit(
            inlets=[silver_dataset],
        )
        def validate_uniquiness() -> bool:
            con = duckdb.connect(database=":memory:")

            con.execute(
                generate_id_uniquiness_query(
                    read_filepath=f"{silver_dataset.uri}/**/*.parquet"
                )
            )

            data = con.fetchall()

            con.close()

            return data == []

        @task.short_circuit(
            inlets=[silver_dataset],
        )
        def validate_brewery_type() -> bool:
            con = duckdb.connect(database=":memory:")

            con.execute(
                generate_valid_brewery_type_query(
                    read_filepath=f"{silver_dataset.uri}/**/*.parquet"
                )
            )

            data = con.fetchall()

            con.close()

            return data == []

        [validate_brewery_type(), validate_uniquiness()]  # type: ignore

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

    bronze_evaluator(save_bronze()) >> save_silver() >> data_quality_check() >> save_gold()  # type: ignore
