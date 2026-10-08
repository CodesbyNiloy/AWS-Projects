"""
Zillow analytics DAG
====================
RapidAPI (Zillow) -> local JSON -> S3 landing bucket -> (Lambda: clean/transform)
-> S3 cleaned bucket (CSV) -> Redshift.

Tasks
-----
1. tsk_extract_zillow_data_var   PythonOperator         call API, write JSON to disk
2. tsk_load_to_s3                BashOperator           move JSON to the landing bucket
3. tsk_is_file_in_s3_available   S3KeySensor            wait for Lambda to drop the CSV
4. tsk_transfer_s3_to_redshift   S3ToRedshiftOperator   COPY the CSV into Redshift

Config (env vars, with defaults) and Airflow Variables:
    ZILLOW_API_URL, ZILLOW_LOCATION, OUTPUT_DIR
    LANDING_BUCKET, CLEANED_BUCKET
    REDSHIFT_SCHEMA, REDSHIFT_TABLE
    AWS_CONN_ID, REDSHIFT_CONN_ID
    Variable `zillow_api_headers` (JSON): {"X-RapidAPI-Key": "...", "X-RapidAPI-Host": "..."}
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

log = logging.getLogger(__name__)

# Config

ZILLOW_API_URL = os.getenv("ZILLOW_API_URL", "https://zillow56.p.rapidapi.com/search")
ZILLOW_LOCATION = os.getenv("ZILLOW_LOCATION", "houston, tx")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/home/ubuntu")

LANDING_BUCKET = os.getenv("LANDING_BUCKET", "my-landing-zone-bucket")
CLEANED_BUCKET = os.getenv("CLEANED_BUCKET", "cleaned-data-zone-csv-bucket")

REDSHIFT_SCHEMA = os.getenv("REDSHIFT_SCHEMA", "public")
REDSHIFT_TABLE = os.getenv("REDSHIFT_TABLE", "zillow_data")

AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_s3_conn")
REDSHIFT_CONN_ID = os.getenv("REDSHIFT_CONN_ID", "conn_id_redshift")

EXTRACT_TASK_ID = "tsk_extract_zillow_data_var"


# Task callables

def extract_zillow_data(url: str, querystring: dict, run_ts: str, output_dir: str) -> dict:
    
    headers = Variable.get("zillow_api_headers", deserialize_json=True)

    response = requests.get(url, headers=headers, params=querystring, timeout=30)
    response.raise_for_status()
    payload = response.json()

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / f"response_data_{run_ts}.json"
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)

    log.info("Wrote %s", json_path)
    return {
        "json_path": str(json_path),
        "csv_key": f"response_data_{run_ts}.csv",
    }


# DAG

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 1),
    "email": ["myemail@domain.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
}

with DAG(
    dag_id="zillow_analytics_dag",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["zillow", "s3", "redshift"],
) as dag:

    extract_zillow_data_var = PythonOperator(
        task_id=EXTRACT_TASK_ID,
        python_callable=extract_zillow_data,
        op_kwargs={
            "url": ZILLOW_API_URL,
            "querystring": {"location": ZILLOW_LOCATION},
            "run_ts": "{{ ts_nodash }}",  # rendered per run, not at parse time
            "output_dir": OUTPUT_DIR,
        },
    )

    load_to_s3 = BashOperator(
        task_id="tsk_load_to_s3",
        bash_command=(
            'aws s3 mv "{{ ti.xcom_pull(task_ids="' + EXTRACT_TASK_ID + '")["json_path"] }}" '
            '"s3://${LANDING_BUCKET}/"'
        ),
        env={"LANDING_BUCKET": LANDING_BUCKET},
        append_env=True,
    )

    is_file_in_s3_available = S3KeySensor(
        task_id="tsk_is_file_in_s3_available",
        bucket_key='{{ ti.xcom_pull(task_ids="' + EXTRACT_TASK_ID + '")["csv_key"] }}',
        bucket_name=CLEANED_BUCKET,
        aws_conn_id=AWS_CONN_ID,
        wildcard_match=False,
        mode="reschedule",   # frees the worker slot between pokes
        poke_interval=15,
        timeout=600,
    )

    transfer_s3_to_redshift = S3ToRedshiftOperator(
        task_id="tsk_transfer_s3_to_redshift",
        aws_conn_id=AWS_CONN_ID,
        redshift_conn_id=REDSHIFT_CONN_ID,
        s3_bucket=CLEANED_BUCKET,
        s3_key='{{ ti.xcom_pull(task_ids="' + EXTRACT_TASK_ID + '")["csv_key"] }}',
        schema=REDSHIFT_SCHEMA,
        table=REDSHIFT_TABLE,
        copy_options=["csv", "IGNOREHEADER 1"],
        method="APPEND",
    )

    extract_zillow_data_var >> load_to_s3 >> is_file_in_s3_available >> transfer_s3_to_redshift