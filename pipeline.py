from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from download import download_trips
from jobs.bronze_t_silver import bronze_to_silver
from jobs.silver_t_gold import silver_to_gold


with DAG(
    dag_id="chicago_taxi_medallion",
    description="Chicago Taxi ingestion and medallion pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["pyspark", "medallion", "chicago-taxi"],
) as dag:

    ingest = PythonOperator(
        task_id="download_to_bronze",
        python_callable=download_trips,
    )

    transform = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=bronze_to_silver,
    )

    aggregate = PythonOperator(
        task_id="silver_to_gold",
        python_callable=silver_to_gold,
    )

    ingest >> transform >> aggregate