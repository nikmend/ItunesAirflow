

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from operators.SnowflakeCustomOperator import SnowflakeCustomOperator
from dbUtils import getDIMDict,  updateCollections, updateArtists, updateTracks
from scripts.scripts import fact_top

# other packages

from datetime import timedelta, datetime

dimTables=['DIM_COUNTRY','DIM_CURRENCY','DIM_EXPLICITNESS','DIM_GENRE','DIM_KIND']
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
def getDicts(ti, **kwargs):
    tab_name = kwargs.get('table')
    dictVal=getDIMDict(tab_name)
    ti.xcom_push(key='DIMDATA', value=dictVal)

def callUpdateCollections(ti, **kwargs):
    dimDicts={}
    for tab in dimTables:
        dimDicts[tab]=ti.xcom_pull(key='DIMDATA', task_ids=tab)
    print(dimDicts)
    inserts=updateCollections(dimDicts)
    return inserts


def callUpdateTracks(ti, **kwargs):
    dimDicts={}
    for tab in dimTables:
        dimDicts[tab]=ti.xcom_pull(key='DIMDATA', task_ids=tab)
    print(dimDicts)
    inserts=updateTracks(dimDicts)
    return inserts

def callUpdateArtists(ti, **kwargs):
    inserts=updateArtists()
    return inserts

    

dag = DAG(
   dag_id='starModel',
   description='',
   default_args=default_args)


dim_collections = PythonOperator(dag=dag,
        task_id='updateCollections',
        provide_context=True,
        python_callable=callUpdateCollections)

dim_artists = PythonOperator(dag=dag,
        task_id='updateArtists',
        provide_context=True,
        python_callable=callUpdateArtists)
fact_tracks = PythonOperator(dag=dag,
        task_id='factTracks',
        provide_context=True,
        python_callable=callUpdateTracks)

for tab in dimTables:    
    tmp = PythonOperator(dag=dag,
        task_id=tab,
        python_callable=getDicts,
        provide_context=True,
        op_kwargs={'table':tab})
    tmp>>dim_artists

trigger_snowflake_pipeline = SnowflakeCustomOperator(
   task_id = 'fact_snowflake',
   sql=fact_top,
   dag=dag
   )




dim_artists>>dim_collections
[dim_artists,dim_collections]>>fact_tracks
fact_tracks>>trigger_snowflake_pipeline