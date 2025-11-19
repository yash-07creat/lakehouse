# lakehouse_pipeline.py (Airflow DAG)
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('lakehouse_daily', start_date=datetime(2024,1,1), schedule_interval='@daily', catchup=False) as dag:
    gen = BashOperator(task_id='generate', bash_command='python /path/to/repo/data/synthetic_generator.py')
    ingest = BashOperator(task_id='ingest', bash_command='python /path/to/repo/ingestion/autoloader_ingest.py')
    bronze = BashOperator(task_id='bronze', bash_command='python /path/to/repo/bronze/bronze_transform.py')
    silver = BashOperator(task_id='silver', bash_command='python /path/to/repo/silver/silver_clean.py')
    gold = BashOperator(task_id='gold', bash_command='python /path/to/repo/gold/gold_aggregates.py')

    gen >> ingest >> bronze >> silver >> gold
