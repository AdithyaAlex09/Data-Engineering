import sys
import os 
import pandas as pd
import numpy as np
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from io import StringIO
from io import BytesIO

path_add="/opt/airflow/"
sys.path.append(path_add)
import boto3

# enviornment varabiles 
file_link="/opt/airflow/data/banking.csv"
default_args = {
    'owner': 'ADITHYA',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Define the DAG with its structure and tasks
dag = DAG('BANK_DATA', 
          default_args=default_args, 
          description='BANK ETL PIPLELINE',
          schedule_interval=timedelta(days=1))

# Define tasks
def data_to_minio():
    data=pd.read_csv(file_link)
     # Convert DataFrame to CSV string
    print(data.head(5))
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    
    # Reset the buffer position to the beginning
    csv_buffer.seek(0)
    s3 = boto3.client('s3',  endpoint_url='http://host.docker.internal:9000',aws_access_key_id='HwvuIsZomwbM57xBJ7A6',aws_secret_access_key='rW29KzfyfWfC0Eff1AFIkIRBHZ14y9qY0lNRJ8ns')
    # Define the bucket name
    bucket_name = 'rawdata-bank'
    # Iterate over each file and upload to MinIO
    
    object_name =  "banking.csv"
    try:
        # Upload file to MinIO
        s3.put_object(Bucket=bucket_name, Key=object_name, Body=csv_buffer.getvalue())
        print(f"File '{object_name}' uploaded successfully to bucket '{bucket_name}'")
    except Exception as e:
        print(e)
        

def extract_data_from_minio(bucket_name, object_name,file_path):
    # Initialize the S3 client
    s3 = boto3.client('s3',
                      endpoint_url='http://host.docker.internal:9000',
                      aws_access_key_id='HwvuIsZomwbM57xBJ7A6',
                      aws_secret_access_key='rW29KzfyfWfC0Eff1AFIkIRBHZ14y9qY0lNRJ8ns')

    try:
        # Download object from MinIO
        response = s3.get_object(Bucket=bucket_name, Key=object_name)
        # Read the contents of the response into a BytesIO buffer
        object_data = BytesIO(response['Body'].read())
        # Convert BytesIO buffer to pandas DataFrame
        data = pd.read_csv(object_data)
        data.to_csv(file_path)
        return data
    except Exception as e:
        print(f"Error extracting data: {e}")
        return None

def transform_data(data):
    # Example transformation: convert column names to lowercase
    data.columns = map(str.lower, data.columns)
    return data

def load_data_to_etl(data):
    # Example: perform ETL operations (e.g., load data into a database)
    print("ETL process completed successfully.")
    # In a real scenario, you would perform your ETL operations here




def minio_to_ETL():
    bucket_name = 'rawdata-bank'    
    object_name = 'banking.csv'
    file_path = "/opt/airflow/data/transformed_data.csv"
        
        # Example usa

    # Extract data from MinIO
    extracted_data = extract_data_from_minio(bucket_name, object_name,file_path)
    if extracted_data is not None:
        print("Data extracted successfully.")
        # Transform extracted data
        transformed_data = transform_data(extracted_data)
        # Load transformed data into ETL pipeline
        load_data_to_etl(transformed_data)
    else:
        print("Failed to extract data.")


  
raw_data = PythonOperator(task_id='load_raw_data', python_callable=data_to_minio, dag=dag)


ETL_data = PythonOperator(task_id='Extract_data', python_callable=minio_to_ETL,dag=dag)
# goodbye_task = PythonOperator(task_id='goodbye_task', python_callable=print_goodbye, dag=dag)
# end_task = DummyOperator(task_id='end', dag=dag)

# Define task dependencies
raw_data>>ETL_data #>> hello_task >> goodbye_task >> end_task