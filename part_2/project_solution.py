from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

GLOBAL_NAME = 'msolonin'
DAGS_PATH = '/opt/airflow/dags'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1, 0, 0),
}

with DAG(
        'project_solution',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=[GLOBAL_NAME]
) as dag:
    landing_to_bronze = SparkSubmitOperator(
        task_id='landing_to_bronze',
        application=f'{DAGS_PATH}/landing_to_bronze.py',
        name='landing_to_bronze',
        conn_id='spark_default',
    )
    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application=f'{DAGS_PATH}/bronze_to_silver.py',
        name='bronze_to_silver',
        conn_id='spark_default',
    )
    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application=f'{DAGS_PATH}/silver_to_gold.py',
        name='silver_to_gold',
        conn_id='spark_default',
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
