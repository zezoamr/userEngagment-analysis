from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators import spark_submit 
from airflow.operators.empty import EmptyOperator
from mockdata_gen import mockdata
from spark_analysis import spark_analysis
from time import sleep

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 12, 9),
    'retries': 1,
    'depends_on_past': False,
    'retry_delay': timedelta(seconds=10)
    #, 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2023, 1, 1),
}

with DAG('UserEngagment',
        default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    d = mockdata()
    s = spark_analysis()
    
    def finishdag():
        abortTime = 1*60 # a minute for testing, in the real pipeline probably like 23.5 hours
        sleep(abortTime)
        d.terminate()
        s.terminate()
    
    
    
    data = PythonOperator(task_id='mock_data', python_callable=d.ksqldb_insertStream())
    spark = spark_submit(task_id='spark_analysis', conn_id= 'spark_local', application=r'D:/projects/kafka_userEngagment/spark_analysis.py'
                                , retries=3)
    finish = PythonOperator(task_id='finish', python_callable=finishdag(), retries=3)
    dummy = EmptyOperator(task_id = 'startdummy')
    dummy >> [data, spark, finish]
    
    
