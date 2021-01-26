#coding:utf-8
# from neo4j import GraphDatabase
import tushare as ts
from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# topic config
# bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test



# replace your token
TSTOKEN = '' 

# tushare token
ts.set_token(TSTOKEN)
pro = ts.pro_api()

# define arges
args = {
    "owner": "ccao",
    "depends_on_past": False
}


# define DAG
dag = DAG(
    dag_id="ExampleKafkaTest",
    catchup=False,
    default_args=args,
    start_date=days_ago(2),
    # schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['tushare','coincap'],
    params={"anykey": "anyvalue"},
)

# 
# you defined some func that you want run
def getDailyCap(**kwargs):
    # get date string
    dtToday = datetime.now()
    todayStr = dtToday.strftime("%Y%m%d") # 'YYYYmmdd'
    # fetch from tushare
    df = pro.query('coincap', trade_date='20180806', coin='BTC')
    print(df)

def callablefunc(**kwargs):
    # Asynchronous by default
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    # configure multiple retries
    producer = KafkaProducer(retries=5)
    future = producer.send('test', b'raw_bytes')
    # produce asynchronously with callbacks
    # producer.send('test', b'raw_bytes').add_callback(on_send_success).add_errback(on_send_error)
    # finish
    # block until all async messages are sent
    result = future.get(timeout=10)
    producer.flush()
    print(result)
    print('------------sent-------------')


def listen(**kwargs):
    # 
    consumer = KafkaConsumer('test', bootstrap_servers=['localhost:9092'])
    for msg in consumer:
        print("%s",msg)
        print('===================::received::==================')
    ### this will keep on listening


task1 = PythonOperator(
    task_id='ExampleKafkaProduce',
    python_callable = callablefunc,
    provide_context=True,
    dag=dag,
)

task2 = PythonOperator(
    task_id='ExampleKafkaConsume',
    python_callable = listen,
    provide_context=True,
    dag=dag,
)

task1 >> task2

if __name__ == "__main__":
    dag.cli()