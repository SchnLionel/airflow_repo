from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.filesystem import FileSensor
from airflow.operators.docker_operator import DockerOperator
from docker.types import Mount
from airflow.operators.postgres_operator import PostgresOperator
from build_init_order import build_init_order

with DAG(
    dag_id='load_order',
    tags=['order', 'docker', 'postgres', 'datascientest'],
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1),
    },
    catchup=False
) as dag:

    # Integration of the initialization task group (Connections + SQL Tables)
    init_order = build_init_order(dag)

    # Sensor to check for the source file
    orders_sensor = FileSensor(
        task_id='orders_sensor',
        fs_conn_id='fs_default', # Will be created by init_order
        filepath='data/to_ingest/bronze/orders.json',
        poke_interval=20,
        timeout=120,
        mode='poke'
    )

    # Transformation step using DockerOperator
    python_transform = DockerOperator(
        task_id='python_transform',
        image='python_transform:latest',
        auto_remove=True,
        command='python3 main.py',
        mounts=[
            Mount(source='/home/ubuntu/airflow_repo/data/to_ingest', target='/app/data/to_ingest', type='bind')
        ]
    )

    # Loading step using DockerOperator
    python_load = DockerOperator(
        task_id='python_load',
        image='python_load:latest',
        auto_remove=True,
        environment={
            'HOST': 'postgres',
            'DATABASE': 'airflow',
            'USER': 'airflow',
            'PASSWORD': 'airflow'
        },
        command='python3 main.py',
        network_mode='airflow_repo_default',
        mounts=[
            Mount(source='/home/ubuntu/airflow_repo/data/to_ingest', target='/app/data/to_ingest', type='bind')
        ]
    )

    # Dependencies
    init_order >> orders_sensor >> python_transform >> python_load
