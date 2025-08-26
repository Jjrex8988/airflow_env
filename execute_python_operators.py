# nano ~/airflow/dags/execute_python_operators.py
from datetime import datetime, timedelta

import time

from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'jjrex8988',
}


# 1
# def print_function():
#     print("The simplest possible Python Operator!")
#
#
# with DAG(
#         dag_id='executing_python_operator',
#         description='Python operators in DAGs',
#         default_args=default_args,
#         start_date=days_ago(1),
#         schedule_interval='@daily',
#         tags=['simple, python']
# ) as dag:
#     task = PythonOperator(
#         task_id="python_task",
#         python_callable=print_function
#     )
#
# task


# 2
# def task_a():
#     print('TASK A executed!')
#
#
# def task_b():
#     time.sleep(5)
#     print('TASK B executed!')
#
#
# def task_c():
#     print('TASK C executed!')
#
#
# def task_d():
#     print('TASK D executed!')
#
#
# with DAG(
#         dag_id='executing_python_operator',
#         description='Python operators in DAGs',
#         default_args=default_args,
#         start_date=days_ago(1),
#         schedule_interval='@daily',
#         tags=['dependencies, python']
# ) as dag:
#     taskA = PythonOperator(
#         task_id="taskA",
#         python_callable=task_a
#     )
#
#     taskB = PythonOperator(
#         task_id="taskB",
#         python_callable=task_b
#     )
#
#     taskC = PythonOperator(
#         task_id="taskC",
#         python_callable=task_c
#     )
#
#     taskD = PythonOperator(
#         task_id="taskD",
#         python_callable=task_d
#     )
# taskA >> [taskB, taskC]
# [taskB, taskC] >> taskD


# 3
def great_hello(name):
    print('Hello, {namee}!'.format(namee=name))


def greet_hello_with_city(namee, cityy):
    print('Hello, {name} from {city}'.format(name=namee, city=cityy))


with DAG(
        dag_id='executing_python_operator',
        description='Python operators in DAGs',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@daily',
        tags=['parametrs, python']
) as dag:
    taskA = PythonOperator(
        task_id="great_hello",
        python_callable=great_hello,
        op_kwargs={'name': 'Jjrex8988'}
    )

    taskB = PythonOperator(
        task_id="greet_hello_with_city",
        python_callable=greet_hello_with_city,
        op_kwargs={'namee': 'Jjrex8988', 'cityy': 'Butterworth'}
    )

taskA >> taskB