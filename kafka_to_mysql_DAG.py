from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'MohamedGamal',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 20),
    'retries': 1,
}

dag = DAG(
    'kafka_to_mysql_python',
    default_args=default_args,
    description='Run Python script using Airflow',
    schedule_interval='@hourly',
    catchup=False,
)

run_PythonTask = BashOperator(
    task_id='run_Python_script',
    bash_command='python /home/gamal/airflow/dags/kafka_to_mysql.py',
    dag=dag,
)

run_PythonTask
