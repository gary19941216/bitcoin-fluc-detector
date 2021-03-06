from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os

# target scala class
scalaClass = ' --class unify.Unify'

# dependency packages for spark
packages = ' --packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.0,' + \
           'com.johnsnowlabs.nlp:spark-nlp_2.11:2.4.2,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3'

# path to jar file
path = ' /usr/local/Insight_Project/bitcoin-fluc-detector/spark/target/scala-2.11/bit_fluc_2.11-1.0.jar'

# spark-submit
sparkSubmit = '/usr/local/spark/bin/spark-submit'

## Define the DAG object
default_args = {
    'owner': 'gary-bitcoin-reddit',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 22, 10, 53),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

# run the scheduled job every 30 minutes
dag = DAG('reddit_bitcoin_dag', default_args=default_args, schedule_interval='*/30 * * * *')

# bash command for scheduled job
command = sparkSubmit + scalaClass + packages + path

# configure job
unify = BashOperator(
    task_id='unify-data',
    bash_command=command,
    dag=dag)
