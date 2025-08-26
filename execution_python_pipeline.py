# nano ~/airflow/dags/execution_python_pipeline.py
# mkdir -p ~/airflow/dag/datasets
import pandas as pd

from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'jjrex8988',
}


def read_csv_filee():
    df = pd.read_csv('/home/jjrex8988/airflow/dags/datasets/insurance.csv')
    # df = pd.read_csv('\home\jjrex8988\airflow\dags\datasets\insurance.csv')
    print(df)

    return df.to_json()


# 1
# def remove_null_values(**kwargs):
#     ti = kwargs['ti']
#     json_data = ti.xcom_pull(task_ids='read_csv_filee')
#     df = pd.read_json(json_data)
#     df = df.dropna()
#     print(df)
#     return df.to_json()
#
#
# with DAG(
#         dag_id='python_pipeline',
#         description='Python pipeline with PythonOperator',
#         default_args=default_args,
#         start_date=days_ago(1),
#         schedule_interval='@once',
#         tags=['python', 'transform', 'pipeline']
# ) as dag:
#     read_csv_filee = PythonOperator(
#         # task_id="read_csv_filee",
#         task_id="read_csv_filee",
#         python_callable=read_csv_filee
#     )
#
#     remove_null_valuess = PythonOperator(
#         task_id="remove_null_values",
#         python_callable=remove_null_values
#     )
#
# read_csv_filee >> remove_null_valuess


# 2
# mkdir -p ~/airflow/dags/output
def remove_null_valuess(ti):
    json_data = ti.xcom_pull(task_ids='read_csv_filee')
    df = pd.read_json(json_data)
    df = df.dropna()
    print(df)

    return df.to_json()


def groupby_smoker(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_valuess')
    df = pd.read_json(json_data)

    smoke_df = df.groupby('smoker').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    smoke_df.to_csv('/home/jjrex8988/airflow/dags/output/grouped_by_smoker.csv', index=False)


def groupby_region(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_valuess')
    df = pd.read_json(json_data)

    region_df = df.groupby('region').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    region_df.to_csv('/home/jjrex8988/airflow/dags/output/grouped_by_region.csv', index=False)


with DAG(
        dag_id='python_pipeline',
        description='Python pipeline with PythonOperator',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['python', 'transform', 'pipeline']
) as dag:
    read_csv_filee = PythonOperator(
        task_id="read_csv_filee",
        python_callable=read_csv_filee
    )

    remove_null_valuess = PythonOperator(
        task_id="remove_null_valuess",
        python_callable=remove_null_valuess
    )

    groupby_smoker = PythonOperator(
        task_id="groupby_smoker",
        python_callable=groupby_smoker
    )

    groupby_region = PythonOperator(
        task_id="groupby_region",
        python_callable=groupby_region
    )

read_csv_filee >> remove_null_valuess >> [groupby_smoker, groupby_region]
