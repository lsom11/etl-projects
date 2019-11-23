from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 11),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('nhlPysparkDag', default_args=default_args, schedule_interval="* * * * *")


def nhl_spark_job():
    import pyspark
    from os import chdir, getenv
    from os.path import abspath
    from pyspark.sql import SQLContext

    # path = chdir(abspath(getenv("HOME") + '/code/personal/datasets/game.csv'))
    path = abspath(getenv("HOME") + '/code/personal/datasets/game.csv')
    sc = pyspark.SparkContext()

    lines = sc.textFile(path)
    count = lines.distinct().count()

    print(count)


spark_job = PythonOperator(
    task_id='spark_job',
    python_callable=nhl_spark_job,
    dag=dag)
