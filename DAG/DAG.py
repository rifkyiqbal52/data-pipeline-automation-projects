import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'Rifky',
    'start_date': dt.datetime(2024, 11, 1, 9, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=10),
}


with DAG('Milestone3',
         default_args=default_args,
         schedule_interval='10-30/10 9 * * 6',
         catchup=False,
         ) as dag:

    python_extract = BashOperator(task_id='python_extract', bash_command='sudo -u airflow python /opt/airflow/scripts/milestone3/extract.py')
    python_transform = BashOperator(task_id='python_transform', bash_command='sudo -u airflow python /opt/airflow/scripts/milestone3/transform.py')
    python_load = BashOperator(task_id='python_load', bash_command='sudo -u airflow python /opt/airflow/scripts/milestone3/load.py')
    

python_extract >> python_transform >> python_load