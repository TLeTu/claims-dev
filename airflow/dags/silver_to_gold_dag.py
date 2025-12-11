import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

env_vars = {
    'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID', ''),
    'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
    'AWS_REGION': os.getenv('AWS_REGION', '')
}

default_args = {
    'description': 'Orchestrate Silver to Gold transformation process',
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

dag = DAG(
    dag_id='silver_to_gold_transformation_dag',
    default_args=default_args,
    schedule=timedelta(hours=24)
)

with dag:
    transform_task = DockerOperator(
        task_id='transform_silver_to_gold_task',
        image='claims-dev-pyspark-app:latest',
        command="spark-submit --master 'local[*]' /opt/spark/apps/transformation/silver_to_gold.py",
        mounts=[
            Mount(
                source='/mnt/HDD/Projects/claims-dev/apps',
                target='/opt/spark/apps',
                type='bind'),
        ],
        environment=env_vars,
        network_mode='claims-dev_spark-kafka-net',
        docker_url='unix://var/run/docker.sock',
        auto_remove='success',
        mount_tmp_dir=False,
    )
