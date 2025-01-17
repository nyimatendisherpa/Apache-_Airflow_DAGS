from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from datetime import datetime

# Define a Dataset pointing to the file
my_file = Dataset("/tmp/my_file.txt")

# Define the DAG
with DAG(
    dag_id="consumer",
    schedule=[my_file],  # Use the Dataset to trigger this DAG
    start_date=datetime(2022, 1, 1),
    catchup=False
):

    # Define a task to read the Dataset
    @task
    def read_dataset():
        with open(my_file.uri, "r") as f:
            print(f.read())

    # Call the task
    read_dataset()
