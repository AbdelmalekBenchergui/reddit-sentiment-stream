from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import time
import os

#Paths
BASE_PATH = "/opt/airflow/src"   
PRODUCER_PATH = f"{BASE_PATH}/producer_faker.py"
SPARK_PATH = f"{BASE_PATH}/spark_consumer.py"

default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="kafka_spark_streaming_orchestration",
    schedule_interval=None,      # manuel
    catchup=False,
    default_args=default_args,
    description="Orchestrate Kafka -> Spark -> Cassandra",
):

    def start_producer():
        subprocess.Popen(["python3", PRODUCER_PATH])
        time.sleep(3)  

    def start_spark_stream():
        subprocess.Popen(["python3", SPARK_PATH])
        time.sleep(5)

    def check_kafka():
        print("Checking Kafka ...")
        time.sleep(2)
        return True

    check_kafka_task = PythonOperator(
        task_id="check_kafka_running",
        python_callable=check_kafka,
    )

    start_producer_task = PythonOperator(
        task_id="start_kafka_producer",
        python_callable=start_producer,
    )

    start_spark_stream_task = PythonOperator(
        task_id="start_spark_consumer",
        python_callable=start_spark_stream,
    )

    check_kafka_task >> start_producer_task >> start_spark_stream_task
