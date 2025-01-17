from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def print_hello():
    print("Hello, Airflow!")

# Define the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 9, 1),
}

with DAG(
    dag_id="hourly_dag",
    default_args=default_args,
    schedule_interval="0 * * * *",  # Every hour
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="print_hello_task",
        python_callable=print_hello,
    )
