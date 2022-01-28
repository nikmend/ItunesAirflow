from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow import settings
from airflow.models import Connection
from bulkLoad import loadDataFromAPI

def populateDB(*args, **kwargs):
    seed = kwargs.get('seed')
    loadDataFromAPI(seed)

def create_conn(conn_id, conn_type, host, login, password, port):
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        login=login,
        password=password,
        port=port
    )
    session = settings.Session()
    conn_name = session\
    .query(Connection)\
    .filter(Connection.conn_id == conn.conn_id)\
    .first()

    if str(conn_name) == str(conn_id):
        return 

    session.add(conn)
    session.commit()
create_conn('tmp_itunes', 'mysql', 'db', 'root', 'root', 3306)
#create_conn('tmp_snowflake', 'snowflake', 'kn12851.us-east-2.aws.snowflakecomputing.com', 'NIKMEND56', 'Parzival5&', 443)



default_arg = {
    'owner': 'airflow',
    'start_date': '2021-07-28',
    'depends_on_past': False,
    'retries':1,
   }
   

dag = DAG('mysql-dag',
          default_args=default_arg,
          schedule_interval='@once')

mysql_task = MySqlOperator(dag=dag,
                           mysql_conn_id='tmp_itunes', 
                           task_id='create_tracks',
                           sql='./scripts/tracks.sql',
                           database='itunes')

run_this = PythonOperator(dag=dag,
    task_id='populate_tracks',
    python_callable=populateDB,
    op_kwargs={'seed':'happy'})

mysql_task >>run_this