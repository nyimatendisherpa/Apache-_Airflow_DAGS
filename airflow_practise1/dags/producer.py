from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime
import os
from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime

# Define the dataset pointing to the file path
my_file = Dataset("/tmp/my_file.txt")

# Define the DAG
with DAG(
    dag_id="producer",
    schedule_interval="@daily",  # Corrected schedule_interval
    start_date=datetime(2022, 1, 1),
    catchup=False
) as dag:  # Added 'as dag' to define the DAG variable properly

    # Define the task to update the dataset
    @task(outlets=[my_file])
    def update_dataset():
        # Open the file and append the update
        with open(my_file.uri, "a+") as f:
            f.write("producer update\n")  # Added newline for better formatting

    # Call the task within the DAG
    update_dataset()

