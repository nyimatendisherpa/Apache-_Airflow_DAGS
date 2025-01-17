from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def task_a():
    print("Executing Task A")

def task_b():
    print("Executing Task B")

def task_c():
    print("Executing Task C")

# Define the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 9, 1),
}

with DAG(
    dag_id="monthly_dag",
    default_args=default_args,
    schedule_interval="0 0 1 * *",  # On the 1st of every month at midnight
    catchup=False,
) as dag:

    task_a = PythonOperator(
        task_id="task_a",
        python_callable=task_a,
    )

    task_b = PythonOperator(
        task_id="task_b",
        python_callable=task_b,
    )

    task_c = PythonOperator(
        task_id="task_c",
        python_callable=task_c,
    )

    # Define dependencies: task_b and task_c depend on task_a
    task_a >> [task_b, task_c]
