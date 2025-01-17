from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def weekly_task():
    print("Weekly task execution")

# Define the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "start_date": datetime(2024, 9, 1),
}

with DAG(
    dag_id="weekly_dag",
    default_args=default_args,
    schedule_interval="0 6 * * 1",  # Every Monday at 6 AM
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="weekly_task",
        python_callable=weekly_task,
    )
