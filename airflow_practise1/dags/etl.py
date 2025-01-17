from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Simulate the extraction of data from a file
def extract_data():
    # In a real pipeline, you would read from a file, API, or database
    extracted_data = {"name": "John", "age": 30, "job": "Engineer"}
    return extracted_data

# Simulate the transformation of data (e.g., data cleaning, manipulation)
def transform_data(**kwargs):
    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(task_ids='extract')
    
    # Simple transformation: making the job uppercase and adding a new field
    transformed_data = {
        "name": extracted_data["name"].upper(),
        "age": extracted_data["age"],
        "job": extracted_data["job"].upper(),
        "country": "USA"
    }
    return transformed_data

# Simulate the loading of data (e.g., insert into a database)
def load_data(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform')
    
    # In a real pipeline, this would be an insert statement to a database
    print(f"Loading data: {transformed_data}")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 9, 1),
}

# Define the DAG
with DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Define tasks for the pipeline
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load_data,
        provide_context=True,
    )

    # Set the task dependencies (ETL order)
    extract_task >> transform_task >> load_task
