from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Function to execute
def first_function_execute():
    print("Hello world")
    return "Hello world"

# Define the DAG
with DAG(
    dag_id="first_dag",
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2024, 7, 24),  # Corrected here
    },
    catchup=False  # Prevents backfill
) as dag:

    # Define the task
    first_function_execute = PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute
    )
