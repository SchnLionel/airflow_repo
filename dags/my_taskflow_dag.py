from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
import random

@task
def function_with_return_and_push():
    # Accès au contexte si besoin de push manuellement avec une clé spécifique
    task_instance = get_current_context()['task_instance']
    value = random.uniform(a=0, b=1)
    task_instance.xcom_push(key="my_xcom_value", value=value)
    return value

@task
def read_data_from_xcom(my_xcom_value):
    # Plus besoin de xcom_pull ! La valeur est passée directement en argument
    print(f"Valeur reçue via décorateur : {my_xcom_value}")

@dag(
    dag_id='taskflow_xcom_dag',
    tags=['tutorial', 'datascientest', 'taskflow'],
    schedule_interval=None,
    start_date=days_ago(0)
)
def my_taskflow_dag():
    # Les dépendances se définissent naturellement par le passage d'arguments
    valeur = function_with_return_and_push()
    read_data_from_xcom(valeur)

# Instanciation du DAG
dag_instance = my_taskflow_dag()
