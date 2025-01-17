from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1,11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'pandas_example',
    default_args=default_args,
    description='A simple Airflow DAG to read a file using pandas',
    schedule_interval=None,  # Set the schedule interval as needed
)
def data_manipulate():
    df=pd.read_csv(r'/opt/airflow/raw_data/bank.csv')
    df['deposit']=df['deposit'].map({'yes':1,'no':0})
    df.to_csv(r'/opt/airflow/processed_data/bank_out.csv')
read_file_task = PythonOperator(
    task_id='read_file_task',
    python_callable=data_manipulate,
    provide_context=True,
    dag=dag,
)
read_file_task
