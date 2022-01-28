from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from operators.SnowflakeCustomOperator import SnowflakeCustomOperator
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

dag = DAG('branchingGenre',description='Run until find a song with substring AS',
          default_args=default_args,
          schedule_interval='*/20 * * * *')

randomTrackName = SnowflakeCustomOperator(
   task_id = 'randomTrackName',
   sql="""select DISTINCT TRACKNAME from FACT.FAC_TRACKS t sample(1 rows);""",
   do_xcom_push=True,
   dag=dag
   )

def findASTrack(ti, **kwargs):
    trackName= ti.xcom_pull(key='return_value', task_ids='randomTrackName')
    print('Checking ', str(trackName).lower())
    if 'as' in str(trackName).lower() :
        return 'hasAS'
    return 'stillSearch'
    
hasToSearch = BranchPythonOperator(
    task_id='hasToSearch',
    python_callable=findASTrack,
    provide_context=True,
    dag=dag
    )
hasAS = DummyOperator(
    task_id='hasAS',
    dag=dag
    )
still = DummyOperator(
    task_id='stillSearch',
    dag=dag
    )
randomTrackName>>hasToSearch
hasToSearch>>[hasAS,still]