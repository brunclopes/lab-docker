from airflow import DAG
from pathlib import Path
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Arquivos jar para a ingestão no s3 e em Delta
jars = "/opt/airflow/plugins/hadoop-aws-3.3.2.jar" "," "/opt/airflow/plugins/aws-java-sdk-bundle-1.11.1026.jar"

default_args = {
    "owner": "Bruno Lopes",
    "start_date": datetime(2022, 12, 20),
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    Path(__file__).stem,
    default_args=default_args,
    catchup=False,
    schedule_interval="0 * * * *",
    max_active_runs=1,
    concurrency=3,
) as dag:

    start = DummyOperator(task_id="Begin")

    t1 = SparkSubmitOperator(
         task_id="Ingestion_Job1_Spark"
        ,application=f"/opt/airflow/dags/scripts/job-1-spark.py"
        ,conn_id="spark_conn"
        ,jars = jars
    )

    end = DummyOperator(task_id="Ending")

    start >> t1 >> end
