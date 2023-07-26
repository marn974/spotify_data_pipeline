from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from etl import run_spotify_etl


default_args={
    "owner": 'airflow',
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 24),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description="Test etl code"
)

run_etl = PythonOperator(
    task_id='complete_etl_spotify',
    python_callable=run_spotify_etl,
    templates_dict={'run_id':'{{ run_id }}'},
    dag=dag
)

run_etl
