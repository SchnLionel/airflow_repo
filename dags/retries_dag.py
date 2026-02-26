from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import datetime

def failed_task():
    raise Exception('This task is designed to fail and trigger retries!')

with DAG(
    dag_id='retries_dag',
    description='Demonstrating retries and retry_delay',
    tags=['tutorial', 'datascientest', 'reliability'],
    schedule_interval=None,
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1),
    },
    catchup=False
) as my_dag:

    task1 = PythonOperator(
        task_id="my_retry_task",
        python_callable=failed_task,
        retries=5,
        retry_delay=datetime.timedelta(seconds=30)
    )
