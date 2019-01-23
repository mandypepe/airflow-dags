from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import time
from random import *

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'python_operator', default_args=default_args, schedule_interval=timedelta(days=1))
hostname = BashOperator (
    task_id = 'hostnamectl',
    bash_command = 'hostnamectl',
    dag = dag ,
)

def print_context ( ds , ** kwargs ):
    print ( kwargs )
    print ( ds )
    return 'Whatever you return gets printed in the logs'


python_oprt = PythonOperator (
    task_id = 'print_the_context' ,
    provide_context = True ,
    python_callable = print_context ,
    dag = dag ,
)
ending = PythonOperator (
    task_id = 'fin' ,
    provide_context = True ,
    python_callable = print_context ,
    dag = dag ,
)

def my_sleeping_function ( random_base):
    time . sleep ( random_base )

def my_function(initil):
    print(float(random()*initil))



for i in range ( 5 ):
    task = PythonOperator (
        task_id = 'sleep_for_' + str ( i ),
        python_callable = my_sleeping_function ,
        op_kwargs = { 'random_base' : float ( i ) / 10 },
        dag = dag )   
    _task = PythonOperator (
            task_id = 'sleep_' + str(i),
            python_callable = my_function ,
            op_kwargs = { 'initil' : float ( i ) * 100 },
            dag = dag )
    hostname>>task>>python_oprt>>_task>>ending