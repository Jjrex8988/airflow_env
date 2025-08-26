# nano ~/airflow/dags/execute_multiple_tasks.py
from datetime import datetime, timedelta, time

from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'jjrex8988',
}

# 1
# with DAG(
#         dag_id='executing_multiple_tasks',
#         description='DAG with multiple tasks and dependencies',
#         default_args=default_args,
#         start_date=days_ago(1),
#         # schedule='@once',
#         schedule_interval=timedelta(days=1),
#         tags=['upstream', 'downstream']
# ) as dag:
#     taskA = BashOperator(
#         task_id="taskA",
#         bash_command="echo TASK A has executed!"
#     )
#
#     taskB = BashOperator(
#         task_id="taskB",
#         bash_command="echo TASK B has executed!"
#     )
# taskA.set_downstream(taskB)
# # taskA.set_upstream(taskB)


# 2
# with DAG(
#         dag_id='executing_multiple_tasks',
#         description='DAG with multiple tasks and dependencies',
#         default_args=default_args,
#         start_date=days_ago(1),
#         schedule_interval=timedelta(days=1),
#         tags=['upstream', 'downstream']
# ) as dag:
#     taskA = BashOperator(
#         task_id="taskA",
#         bash_command="""
#             echo TASK A has started!
#             for i in {1..10}
#             do
#                 echo TASK A printing $i
#             done
#
#             echo TASK A has ended!
#         """
#     )
#
#     taskB = BashOperator(
#         task_id="taskB",
#         bash_command="""
#         echo TASK B has started!
#         sleep 4
#         echo TASK B has ended!
#         """
#     )
#
#     taskC = BashOperator(
#         task_id="taskC",
#         bash_command="""
#         echo TASK C has started!
#         sleep 15
#         echo TASK C has ended!
#         """
#     )
#
#     taskD = BashOperator(
#         task_id="taskD",
#         bash_command="""
#         echo TASK D has completed!"""
#     )
#
# # taskA.set_downstream(taskB)
# # taskA.set_downstream(taskC)
# #
# # taskD.set_upstream(taskB)
# # taskD.set_upstream(taskC)
# #
# # taskA >> taskB  # taskA bit shift task B, task B depends task A
# # taskA >> taskC
# #
# # taskD << taskB
# # taskD << taskC
#
# taskA >> [taskB, taskC]
# taskD << [taskB, taskC]


# # nano ~/airflow/dags/bash_scripts/taskA.sh
#
# # echo TASK A has started!
# #
# # for i in {1..10}
# # do
# #     echo TASK A printing $i
# # done
# #
# # echo TASK A has ended!
#
# # echo TASK B has started!
# # sleep 4
# # exit 99
# # echo TASK B has ended!
#
# # echo TASK C has started!
# # sleep 10
# # exit 130
# # echo TASK C has ended!
#
#
# # echo TASK D has completed!
# # echo TASK E has completed!
# # echo TASK F has completed!
# # echo TASK G has completed!


# 3
with DAG(
        dag_id='executing_multiple_tasks',
        description='DAG with multiple tasks and dependencies',
        default_args=default_args,
        start_date=days_ago(1),
        # schedule='@once',
        schedule_interval=timedelta(days=1),
        tags=['scritps', 'template search'],
        # template_searchpath='~/airflow/dags/bash_scripts',
        template_searchpath='/home/jjrex8988/airflow/dags/bash_scripts'
) as dag:
    taskA = BashOperator(
        task_id="taskA",
        bash_command="taskA.sh"
    )

    taskB = BashOperator(
        task_id="taskB",
        bash_command="taskB.sh"
    )

    taskC = BashOperator(
        task_id="taskC",
        bash_command="taskC.sh"
    )

    taskD = BashOperator(
        task_id="taskD",
        bash_command="taskD.sh"
    )

    taskE = BashOperator(
        task_id="taskE",
        bash_command="taskE.sh"
    )

    taskF = BashOperator(
        task_id="taskF",
        bash_command="taskF.sh"
    )

    taskG = BashOperator(
        task_id="taskG",
        bash_command="taskG.sh"
    )

taskA >> taskB >> taskE
taskA >> taskC >> taskF
taskA >> taskD >> taskG