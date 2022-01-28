
# airflow related

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from operators.SnowflakeCustomOperator import SnowflakeCustomOperator

# other packages

from datetime import timedelta, datetime

default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'start_date': datetime(2021, 8, 4),
   'email_on_failure': False,
   'email_on_retry': False,
   'schedule_interval': '@hourly',
   'retries': 1,
   'retry_delay': timedelta(seconds=5),
}

dag = DAG(
   dag_id='SnowflakeCustomOperator',
   description='',
   default_args=default_args)


trigger_snowflake_pipeline = SnowflakeCustomOperator(
   task_id = 'trigger_snowflake_pipeline',
   sql = './scripts/tracks.sql',
   dag=dag

   )