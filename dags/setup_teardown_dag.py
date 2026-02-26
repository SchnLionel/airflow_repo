import random
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import datetime

def successful_task(name):
    print(f"Task {name} success")

def random_fail_task():
    random.seed()
    if random.random() < 0.5:
        raise Exception('Random failure')

with DAG(
    dag_id='setup_teardown_dag',
    description='Demonstrating setup and teardown tasks',
    tags=['tutorial', 'datascientest', 'resource_management'],
    schedule_interval=None,
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1)
    },
    catchup=False
) as my_dag:

    setup_task = PythonOperator(
        task_id='setup_resources',
        python_callable=successful_task,
        op_kwargs={'name': 'setup'}
    )

    work_task1 = PythonOperator(
        task_id='work_task_1',
        python_callable=random_fail_task
    )

    work_task2 = PythonOperator(
        task_id='work_task_2',
        python_callable=random_fail_task
    )

    teardown_task = PythonOperator(
        task_id='cleanup_resources',
        python_callable=successful_task,
        op_kwargs={'name': 'teardown'}
    )

    # Utilisation des nouveaux opérateurs as_setup et as_teardown
    setup_task.as_setup() >> [work_task1, work_task2]
    [work_task1, work_task2] >> teardown_task.as_teardown()
    
    # Lien direct entre setup et teardown pour garantir l'exécution
    setup_task >> teardown_task
