<h1 align="center">Brewery Location Data Lake</h1>
<p align="center">
<a href="https://github.com/nahumsa/brewery-data-lake/actions"><img alt="Actions Status" src="https://github.com/nahumsa/brewery-data-lake/actions/workflows/ci.yaml/badge.svg"></a>
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
<a href="https://pycqa.github.io/isort/"><img alt="Imports: isort" src="https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336"></a>
<a href="https://github.com/astral-sh/ruff"><img alt="Ruff" src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json"></a>
</p>

## Project Overview

This project fetches data from a brewery API and sets up an Airflow data pipeline to process and store the data in a data lake.

Using the medallion architecture for efficient data management.

## Prerequisites

- Python 3.12 or higher
- pip
- Docker (for running Airflow)

## Installation Guide

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/nahumsa/brewery-data-lake.git
   cd brewery-data-lake
   ```

2. **Install Dependencies**:

   ```bash
   pip install -r requirements.dev.txt
   ```

3. **Set Up Airflow**:
 If it's your first time running, you should run:
`echo -e "AIRFLOW_UID=$(id -u)" > .env`

   ```bash
   docker-compose up
   ```

3. **Access the Airflow UI**:
   - Open a web browser and go to `http://localhost:8080`.
   - Log in using the default credentials (airflow/airflow).

After logging to the airflow UI, you can see the whole data pipeline in the `DAG` called `brewery_dataset`,
or you can see by using the tags attached to the `DAG`, such as `producer`, `brewery`, etc.

## Data Pipeline Overview

The purpose of the pipeline is to fetch data from Open Brewery DB API,
load it to a Data Lake and transform it using a medallion architecture.
The data pipeline steps are illustrated in the following diagram:

```mermaid
flowchart TD
 subgraph s1["Data Lake"]
        n3["Bronze"]
        n4["Silver"]
        n5["Gold"]
  end
    n1["Brewery API"] --> n2["Extract Data"]
    n2 -- Save raw JSON --> n3
    n2 --> n6["Transform Data"]
    n6 --> n7["Aggregate Data"]
    n6 -- DuckDB --> n4
    n7 -- DuckDB --> n5

    n1@{ shape: dbl-circ}
    style n3 stroke:#D50000
    style n4 stroke:#FFD600
    style n5 stroke:#00C853
    style n2 stroke:#000000
    style n6 stroke:#000000
    style n7 stroke:#000000
    style s1 stroke:#000000


```

### Pipeline Observability

Each step of the pipeline is monitored by a given Service Level Agreement (SLA)
that was defined by analyzing the runtime of each task. This will help monitor
the Data Pipeline and also show some potential improvements that could be done
for speeding up the Pipeline. When any SLA is not met, it is possible to
implement an alerting tool such an email or slack message for the responsible
for the pipeline by using the `sla_callback` function.

Also when there is any fail on the pipeline, an alerting can be sent, let's say
by email. This can be set up by adding the SMTP configuration on `airflow.conf`, for instance:

```bash
[email]
email_backend = airflow.utils.email.send_email_smtp
[smtp]
smtp_host = localhost
smtp_starttls = False
smtp_ssl = False
smtp_port = 25
smtp_mail_from = noreply@company.com
```

or you could also: [configure by GMail](<https://helptechcommunity.wordpress.com/2020/04/04/airflow-email-configuration/>).

### Pipeline steps

#### Bronze

In order to save to the bronze layer, we must fetch the data from the Open
Brewery DB API, thus we must use two endpoint, one to understand the metadata
of the data being fetched (quantity, entries per page) and another to
fetch the data. Since Data Quality must be in the forefront, we must make sure
that the we fetch all the data, thus we make a check in the pipeline that all
data is fetched, if the quantity is different from what we gathered with the
API's metadata we should short-circuit the pipeline and then send a notification
for the person responsible for the pipeline.

The Data Quality is assessed by using a
[Pydantic](https://docs.pydantic.dev/latest/) Model
to make sure that columns that are used for partitions
(`country`, `state`, and `city`) are not null.
If any of the entries that are fetched from the API do not comply
with our data model, then the pipeline will fail.

#### Silver

For the silver layer, we must read and process the data that was saved on the bronze layer.

In this pipeline we choose to use DuckDB mainly because it is a lightweight and embedded
database which makes it easier to iterate locally. Another reason is that for the size
of the data that we have we do not need distributed compute such as Apache Spark, however
we could use the [same API as Spark](https://duckdb.org/docs/api/python/spark_api.html)
for transformations by using DuckDB making easier to debug locally spark pipelines without
the arduous set up that Apache Spark needs.

It is performed the following transformations for the dataset:

- Convert `brewery_type`, `city`, `state_province`, `state`, and `country` to lowercase mainly
because there could be upper case differences in the dataset which would not represent the
same values.
- Convert `longitude` and `latitude` to floats because it is important to keep the precision
needed for those variables since the number of decimal places represent the precision. Also
it's important to keep as float because requires less memory.
- Remove `website_url` that have `@gmail.com` which are emails and not URLs.

We also transform the data to Parquet and partition by `country`, `state`, and `city`
which makes it easier to query analytics pipelines regarding the location of the brewery
which is important for multiple reasons, for instance:

- Reducing the amount of data scanned, making cheaper, and speeds up query execution.
- Partitioning divides large datasets into smaller, manageable chunks.
This allows distributed systems to process different partitions in parallel,
leveraging the full power of cluster computing.
- Provides a hierarchical directory structure, making the dataset
easier to understand and manage.
- Partitioning makes it easier to append new data to specific partitions without rewriting
the entire dataset.

In this step of the pipeline we have the following data quality checks:

- `brewery_type` must have the values `'taproom', 'micro', 'contract',
'location', 'planning', 'bar', 'regional', 'large', 'beergarden', 'brewpub',
'proprietor', 'closed', 'nano'`.
- `website_url` must be a properly formatted URL, which starts with `http://`, or null.
- `id` must be unique for all values.
