# ItunesAirflow

Airflow

For the part of the study of the use of Airflow an environment is made by means of Docker, encapsulating a Mysql database 
and the Airflow service to coordinate the use of the Itunes api described above as well as the creation of a star schema in an external  
free Snowflake with the data obtained from the Itunes tracks.

First, the relational model that will represent the original data from the Tracks table is created in Snowflake's external database.

## Dags
Several DAGs were created to gradually appropriate the AIrflow utilities and how to implement them in an existing solution. 

### mysql-Dag
this first dag that is executed only once acts as a creation of the Tracks table in the database using a MySqlOperator, and then inserting random values for the first time in the database using a PythonOperator



### itunesHourly
This dag is only in charge of using a PythonOperator to obtain a random TrackName from the Mysql database, and then insert new api records obtained from that TrackName. This is 
scheduled to be executed every hour
	

### SnowflakeCustomOperator and the Snowflake airflow native operator problem
Due to compatibility problems with the free Snowflake database we had problems using the native Airflow connector because it did not correctly identify the host or the certificate, given the good configurations of the parameters. Due to these difficulties the implementation of a CustomOperator was chosen using the Airflow libraries that allow it.




### StarModel

This DAG works to update the data in the Artist, Collections, Tracks and TopPerGender tables.
Through the xcoms information is passed by each of the dimensions to the following Tasks, inserting information on the artists and collections in Snowflake. This flow is designed not to overwrite existing records as it is handled through time windows in the entries and executions.


### BranchingGenre

This Dag uses the BranchPyhtonOperator, SnowflakeCustomOperator and DummyOperator to follow the following logic: First get a random TrackName from the FACT_Tracks base in SnowFlake, this data is stored and used in the next Operator through task_instanace-xcom.
This name will be verified to have the substring 'as', distributed to the corresponding dummyOperator.

























