from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from bulkLoad import loadDataFromAPI, getRandomArtist

def populateDB(*args, **kwargs):
    loadDataFromAPI(getRandomArtist())


default_arg = {
    'owner': 'airflow',
    'start_date': '2021-07-28',
    'depends_on_past': False,
    'retries':1,
   }
   

dag = DAG('itunes-hourly',
          default_args=default_arg,
          schedule_interval='@hourly')


run_this = PythonOperator(
    dag=dag,
    task_id='populate_tracks',
    python_callable=populateDB)
    
run_this 