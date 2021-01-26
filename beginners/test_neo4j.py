#coding:utf-8
from neo4j import GraphDatabase
import tushare as ts
from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

# replace your token
TSTOKEN = '' 

# neo4j database i/o
uri = "bolt://127.0.0.1:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", ""))

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
    dag_id="TushareDailyCoincap",
    catchup=False,
    default_args=args,
    start_date=days_ago(2),
    # schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['tushare','coincap'],
    params={"anykey": "anyvalue"},
)

# you defined some func that you want run
def getDailyCap(**kwargs):
    # get date string
    dtToday = datetime.now()
    todayStr = dtToday.strftime("%Y%m%d") # 'YYYYmmdd'
    # fetch from tushare
    df = pro.query('coincap', trade_date='20180806', coin='BTC')
    print(df)


def callablefunc(**kwargs):
    df = getDailyCap()

    with driver.session() as session:
        result = session.run("MATCH (n:Person) RETURN n.name AS name")
        print(result)
    return

# this is something that airflow really runs
task1 = PythonOperator(
    task_id='DailyCoinCap',
    python_callable = callablefunc,
    provide_context=True,
    dag=dag,
)

# if multiple operator, define like this
# task1 >> task2

if __name__ == "__main__":
    dag.cli()