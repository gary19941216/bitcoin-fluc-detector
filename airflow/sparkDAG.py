from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os

scalaClass = ' --class unify.Unify'
packages = ' --packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.0,com.johnsnowlabs.nlp:spark-nlp_2.11:2.4.2,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3'
path = ' /usr/local/Insight_Project/bitcoin-fluc-detector/Spark/target/scala-2.11/bit_fluc_2.11-1.0.jar'
sparkSubmit = '/usr/local/spark/bin/spark-submit'

## Define the DAG object
default_args = {
    'owner': 'gary-bitcoin-reddit',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 22, 10, 53),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}
dag = DAG('reddit_bitcoin_dag', default_args=default_args, schedule_interval='*/1 * * * *')

command = command=sparkSubmit + scalaClass + packages + path

unify = BashOperator(
    task_id='unify-data',
    bash_command=command,
    dag=dag)
