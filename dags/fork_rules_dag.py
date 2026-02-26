import random
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import datetime

def successful_task():
    print('success')

def random_fail_task():
    random.seed()
    a = random.random() 
    print(f"Random value: {a}")
    if a < 0.9:
        raise Exception('This task randomly failed (90% chance)')

with DAG(
    dag_id='fork_rules_dag',
    description='Demonstrating trigger_rules (all_failed, all_success, all_done)',
    tags=['tutorial', 'datascientest', 'logic'],
    schedule_interval=datetime.timedelta(seconds=60),
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1)
    },
    catchup=False
) as my_dag:

    task1 = PythonOperator(
        task_id='task1_input',
        python_callable=random_fail_task
    )

    # Se lance uniquement si task1 échoue
    task2_on_failure = PythonOperator(
        task_id='task2_if_failed',
        python_callable=successful_task,
        trigger_rule='all_failed'
    )

    # Se lance uniquement si task1 réussit
    task3_on_success = PythonOperator(
        task_id='task3_if_success',
        python_callable=successful_task,
        trigger_rule='all_success'
    )

    # Se lance dans tous les cas après task2 ou task3
    task4_final = PythonOperator(
        task_id='task4_final',
        python_callable=successful_task,
        trigger_rule='all_done'
    )

    task1 >> [task2_on_failure, task3_on_success]
    [task2_on_failure, task3_on_success] >> task4_final
