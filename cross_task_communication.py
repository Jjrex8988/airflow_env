# nano ~/airflow/dags/cross_task_communication.py
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'jjrex8988',
}


# 1
# def increment_by_1(counterr):
#     print("Count {counter}".format(counter=counterr))
#     return counterr + 1
#
#
# def multiply_by_100(counterrr):
#     print("Count {counter}".format(counter=counterrr))
#     return counterrr * 100
#
#
# with DAG(
#         dag_id='cross_task_coummnication',
#         description='Cross-task communication with XCOM',
#         default_args=default_args,
#         start_date=days_ago(1),
#         schedule_interval='@daily',
#         tags=['xcom, python']
# ) as dag:
#     taskA = PythonOperator(
#         task_id="increment_by_1",
#         python_callable=increment_by_1,
#         op_kwargs={'counterr': 100}
#     )
#
#     taskB = PythonOperator(
#         task_id="multiply_by_100",
#         python_callable=multiply_by_100,
#         op_kwargs={'counterrr': 9}
#     )
#
# taskA  >> taskB


# 2
def increment_by_1(valuee):
    print("Value {value}".format(value=valuee))

    return valuee + 1


# TI = Task Instance, whenever an operator is executed in TaskFlow, it is executed as Task Instance
def multiply_by_100(ti):
    value100 = ti.xcom_pull(task_ids='increment_by_1')
    print("Value {value}".format(value=value100))

    return value100 * 100


def subtract_9(ti):
    value100s9 = ti.xcom_pull(task_ids='multiply_by_100')
    print("Value {value}".format(value=value100s9))
    return value100s9 - 9


def print_value(ti):
    fvalue = ti.xcom_pull(task_ids='subtract_9')

    print("Value {value}".format(value=fvalue))


with DAG(
        dag_id='cross_task_coummnication',
        description='Cross-task communication with XCOM',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@daily',
        tags=['xcom, python']
) as dag:
    increment_by_1 = PythonOperator(
        task_id="increment_by_1",
        python_callable=increment_by_1,
        op_kwargs={'valuee': 1}
    )

    multiply_by_100 = PythonOperator(
        task_id="multiply_by_100",
        python_callable=multiply_by_100
    )

    subtract_9 = PythonOperator(
        task_id="subtract_9",
        python_callable=subtract_9,
    )

    print_value = PythonOperator(
        task_id="print_value",
        python_callable=print_value
    )

increment_by_1 >> multiply_by_100 >> subtract_9 >> print_value