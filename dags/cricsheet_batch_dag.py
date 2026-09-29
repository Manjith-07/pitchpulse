"""
Batch pipeline DAG: load Cricsheet match data into the raw Postgres tables, then
run dbt to build the staging and mart models on top of it.

Schedule is set to `@daily` for demonstration purposes. A real cricket season
only produces new Cricsheet data every few days at most (and the IPL season
itself only runs a couple of months a year), so a production version of this
DAG would use a much sparser schedule (or be triggered manually/by a sensor
watching for new Cricsheet releases) rather than running every day.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "pitchpulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pitchpulse_batch_pipeline",
    description="Load Cricsheet data into Postgres and build dbt models",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["pitchpulse", "batch"],
) as dag:

    load_raw_data = BashOperator(
        task_id="load_raw_data",
        bash_command=(
            "python /opt/airflow/ingestion/batch/load_cricsheet.py "
            "--input-dir /opt/airflow/data/ipl_raw"
        ),
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        # dbt lives in its own virtualenv (/opt/dbt_venv) baked into the
        # Airflow image, isolated from Airflow's own dependencies -- see
        # airflow/Dockerfile for why.
        bash_command="cd /opt/airflow/dbt/pitchpulse && /opt/dbt_venv/bin/dbt build --profiles-dir .",
    )

    load_raw_data >> run_dbt
