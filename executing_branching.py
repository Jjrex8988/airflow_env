# conditional branching, when you want to execute one branch of your workflow/one branch of your DAG
# based on condition that you evaluate, result of conditional will determine what path you take
# nano ~/airflow/dags/executing_branching.py
from datetime import datetime, timedelta

import pandas as pd

from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator, BranchPythonOperator

from airflow.models import Variable

from airflow.utils.task_group import TaskGroup

from airflow.utils.edgemodifier import Label

from random import choice

default_args = {
    'owner': 'jjrex8988',
}

# 1
# def has_driving_license():
#     return choice([True, False])
#
#
# def branch(ti):
#     if ti.xcom_pull(task_ids='has_driving_license'):
#         return 'eligible_to_drive'
#     else:
#         return 'not_eligible_to_drive'
#
#
# def eligible_to_drive():
#     print("You can drive, you have a license!")
#
#
# def not_eligible_to_drive():
#     print("I'm afraid you are out of luck, you need a license to drive")
#
# with DAG(
#         dag_id='executing_branching',
#         description="Running branching pipeline",
#         default_args=default_args,
#         start_date=days_ago(1),
#         schedule_interval="@once",
#         tags=['branching', 'conditions']
# ) as dag:
#     taskA = PythonOperator(
#         task_id='has_driving_license',
#         python_callable=has_driving_license,
#     )
#
#     taskB = BranchPythonOperator(
#         task_id='branch',
#         python_callable=branch,
#     )
#
#     taskC = PythonOperator(
#         task_id='eligible_to_drive',
#         python_callable=eligible_to_drive,
#     )
#
#     taskD = PythonOperator(
#         task_id='not_eligible_to_drive',
#         python_callable=not_eligible_to_drive,
#     )
#
# taskA >> taskB >> [taskC, taskD]

DATASETS_PATH = '/home/jjrex8988/airflow/dags/datasets/insurance.csv'
OUTPUT_PATH = '/home/jjrex8988/airflow/dags/output/{0}.csv'


# 2
# def read_csv_file():
#     df = pd.read_csv(DATASETS_PATH)
#
#     print(df)
#
#     return df.to_json()

# def remove_null_values(ti):
#     json_data = ti.xcom_pull(task_ids='read_csv_file')
#
#     df = pd.read_json(json_data)
#
#     df = df.dropna()
#
#     print(df)
#
#     return df.to_json()

# def determine_branch():
#     transform_action = Variable.get("transform_action", default_var=None)
#
#     if transform_action.startswith('filter'):
#         return transform_action  # transform_action string itself specifies the task ID to execute which is taskid=filter_by_southwest
#     elif transform_action == 'groupby_region_smoker':
#         return 'groupby_region_smoker'
#
#
# def filter_by_southwest(ti):
#     json_data = ti.xcom_pull(task_ids='remove_null_values')
#     df = pd.read_json(json_data)
#     region_df = df[df['region'] == 'southwest']
#
#     region_df.to_csv(OUTPUT_PATH.format('southwest'), index=False)
#
#
# def filter_by_southeast(ti):
#     json_data = ti.xcom_pull(task_ids='remove_null_values')
#     df = pd.read_json(json_data)
#     region_df = df[df['region'] == 'southeast']
#
#     region_df.to_csv(OUTPUT_PATH.format('southeast'), index=False)
#
#
# def filter_by_northwest(ti):
#     json_data = ti.xcom_pull(task_ids='remove_null_values')
#     df = pd.read_json(json_data)
#     region_df = df[df['region'] == 'northwest']
#
#     region_df.to_csv(OUTPUT_PATH.format('northwest'), index=False)
#
#
# def filter_by_northeast(ti):
#     json_data = ti.xcom_pull(task_ids='remove_null_values')
#     df = pd.read_json(json_data)
#     region_df = df[df['region'] == 'northeast']
#
#     region_df.to_csv(OUTPUT_PATH.format('northeast'), index=False)
#
#
# def groupby_region_smoker(ti):
#     json_data = ti.xcom_pull(task_ids='remove_null_values')
#     df = pd.read_json(json_data)
#
#     region_df = df.groupby('region').agg({
#         'age': 'mean',
#         'bmi': 'mean',
#         'charges': 'mean'
#     }).reset_index()
#
#     region_df.to_csv(OUTPUT_PATH.format('group_by_region'), index=False)
#
#     smoker_df = df.groupby('smoker').agg({
#         'age': 'mean',
#         'bmi': 'mean',
#         'charges': 'mean'
#     }).reset_index()
#
#     smoker_df.to_csv(OUTPUT_PATH.format('group_by_smoker'), index=False)
#
# with DAG(
#         dag_id='executing_branching',
#         description="Running a branching pipeline",
#         default_args=default_args,
#         start_date=days_ago(1),
#         schedule_interval="@once",
#         tags=['python', 'transform', 'pipeline', 'branching']
# ) as dag:
#     read_csv_file = PythonOperator(
#         task_id='read_csv_file',
#         python_callable=read_csv_file
#     )
#
#     remove_null_values = PythonOperator(
#         task_id='remove_null_values',
#         python_callable=remove_null_values
#     )
#
#     determine_branch = BranchPythonOperator(
#         task_id='determine_branch',
#         python_callable=determine_branch
#     )
#
#     filter_by_southwest = PythonOperator(
#         task_id='filter_by_southwest',
#         python_callable=filter_by_southwest
#     )
#
#     filter_by_southeast = PythonOperator(
#         task_id='filter_by_southeast',
#         python_callable=filter_by_southeast
#     )
#
#     filter_by_northwest = PythonOperator(
#         task_id='filter_by_northwest',
#         python_callable=filter_by_northwest
#     )
#
#     filter_by_northeast = PythonOperator(
#         task_id='filter_by_northeast',
#         python_callable=filter_by_northeast
#     )
#
#     groupby_region_smoker = PythonOperator(
#         task_id='groupby_region_smoker',
#         python_callable=groupby_region_smoker
#     )
#
# read_csv_file >> remove_null_values >> determine_branch >> [filter_by_southwest,
#                                                             filter_by_southeast,
#                                                             filter_by_northwest,
#                                                             filter_by_northeast,
#                                                             groupby_region_smoker]


# 3
# nano ~/airflow/dags/executing_branching.py

def read_csv_file(ti):
    df = pd.read_csv(DATASETS_PATH)

    print(df)

    ti.xcom_push(key='my_csv',
                 value=df.to_json())  # To push the json value want to pass on the other tasks with the key 'my_csv'


def remove_null_values(ti):
    json_data = ti.xcom_pull(key='my_csv')

    df = pd.read_json(json_data)

    df = df.dropna()

    print(df)

    ti.xcom_push(key='my_clean_csv', value=df.to_json())


def determine_branch():
    transform_action = Variable.get("transform_action", default_var=None)

    if transform_action.startswith('filter'):
        return "filtering.{0}".format(
            transform_action)  # TaskID startwith 'filtering'...filtering here is the name of the task group
    # when you use task group, when you reference a task, you have to specify the name of the task group as PREFIX
    elif transform_action == 'groupby_region_smoker':
        return "grouping.{0}".format(transform_action)


def filter_by_southwest(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)
    region_df = df[df['region'] == 'southwest']

    region_df.to_csv(OUTPUT_PATH.format('southwest'), index=False)


def filter_by_southeast(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)
    region_df = df[df['region'] == 'southeast']

    region_df.to_csv(OUTPUT_PATH.format('southeast'), index=False)


def filter_by_northwest(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)
    region_df = df[df['region'] == 'northwest']

    region_df.to_csv(OUTPUT_PATH.format('northwest'), index=False)


def filter_by_northeast(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)
    region_df = df[df['region'] == 'northeast']

    region_df.to_csv(OUTPUT_PATH.format('northeast'), index=False)


def groupby_region_smoker(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)

    region_df = df.groupby('region').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    region_df.to_csv(OUTPUT_PATH.format('group_by_region'), index=False)

    smoker_df = df.groupby('smoker').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    smoker_df.to_csv(OUTPUT_PATH.format('group_by_smoker'), index=False)


with DAG(
        dag_id='taskgroups_and_edgelabels',
        description="Running a branching pipeline with TaskGroups and EdgeLabels",
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval="@once",
        tags=['python', 'taskgroups', 'edgelabels', 'branching']
) as dag:
    with TaskGroup('reading_and_preprocessing') as reading_and_preprocessing:

        read_csv_file = PythonOperator(
            task_id='read_csv_file',
            python_callable=read_csv_file
        )

        remove_null_values = PythonOperator(
            task_id='remove_null_values',
            python_callable=remove_null_values
        )

        read_csv_file >> remove_null_values

    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch
    )

    with TaskGroup('filtering') as filtering:

        filter_by_southwest = PythonOperator(
            task_id='filter_by_southwest',
            python_callable=filter_by_southwest
        )

        filter_by_southeast = PythonOperator(
            task_id='filter_by_southeast',
            python_callable=filter_by_southeast
        )

        filter_by_northwest = PythonOperator(
            task_id='filter_by_northwest',
            python_callable=filter_by_northwest
        )

        filter_by_northeast = PythonOperator(
            task_id='filter_by_northeast',
            python_callable=filter_by_northeast
        )
    with TaskGroup('grouping') as grouping:

        groupby_region_smoker = PythonOperator(
            task_id='groupby_region_smoker',
            python_callable=groupby_region_smoker
        )

reading_and_preprocessing >> Label('preprocessed data') >> determine_branch >> Label('branch on condition') >> [filtering, grouping]

# Organize complex workflow using task groups and edge labels
# Task Groups allow you to group related tasks together within DAG, 让其容易明白
# Edge Label allow you to add labels to the edge between tasks in a DAG, providing information about the dependencies between tasks
