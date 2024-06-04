from airflow import DAG
from datetime import datetime, timedelta
import requests
import json
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Define the S3 bucket names
s3_bucket = '311-cleaned-bucket'
raw_s3_bucket = '311-raw-bucket'


# Create a unique timestamp for file naming
now = datetime.now()
dt_now_string = now.strftime("%d%m%Y%H%M%S")

def extract_311_data(**kwargs):
    url = kwargs['url']
    dt_string = kwargs['date_string']
    
    response = requests.get(url)
    response_data = response.json()
    
    # specify the output file path
    output_file_path = f"/home/ubuntu/response_data_{dt_string}.json"
    file_str = f'response_data_{dt_string}.csv'
    
    # Write the JSON response to a file
    with open(output_file_path, 'w') as output_file:
        json.dump(response_data, output_file, indent=4)
        
    print(f"Data saved to {output_file_path}")

    output_list = [output_file_path, file_str]
    return output_list

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

with DAG(
    dag_id='extract_311_data_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=True
) as dag:

    extract_311_data_task = PythonOperator(
        task_id='extract_311_data',
        python_callable=extract_311_data,
        op_kwargs={
            'url': f"https://data.cityofnewyork.us/resource/erm2-nwe9.json?$limit=1500",
            'date_string': dt_now_string
        }
    )
    
    load_to_s3_task = BashOperator(
        task_id='load_to_s3',
        bash_command='aws s3 mv {{ ti.xcom_pull(task_ids="extract_311_data")[0] }} s3://311-raw-bucket/',
    )
    
    is_file_in_s3_available_task = S3KeySensor(
        task_id='is_file_in_s3_available',
        bucket_key='{{ ti.xcom_pull("extract_311_data")[1] }}',
        bucket_name=raw_s3_bucket,
        aws_conn_id='aws_connection_id',
        wildcard_match=False,
        timeout=300,
        poke_interval=20,
    )

    create_staging_table_task = PostgresOperator(
        task_id='create_staging_table',
        postgres_conn_id='redshift_conn_id',
        sql="""
            CREATE TABLE IF NOT EXISTS sr.staging_service_requests (
                unique_key INTEGER NOT NULL PRIMARY KEY, 
                created_date TIMESTAMP NOT NULL,
                closed_date TIMESTAMP,
                agency VARCHAR(256),
                agency_name VARCHAR(256),
                complaint_type VARCHAR(256),
                descriptor VARCHAR(256),
                location_type VARCHAR(256),
                incident_address VARCHAR(256),
                city VARCHAR(256),
                borough VARCHAR(256),
                incident_zip INTEGER,
                landmark VARCHAR(256),
                status VARCHAR(256),
                resolution_description VARCHAR(1024),
                resolution_action_updated_date TIMESTAMP,
                open_data_channel_type VARCHAR(256),
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION
            );
        """
    )
        
    transfer_s3_to_redshift_staging_task = S3ToRedshiftOperator(
        task_id='transfer_s3_to_redshift_staging',
        aws_conn_id='aws_connection_id',
        redshift_conn_id='redshift_conn_id',
        s3_bucket=s3_bucket,
        s3_key='{{ ti.xcom_pull("extract_311_data")[1] }}',
        schema='sr',
        table='staging_service_requests',
        copy_options=['csv IGNOREHEADER 1'],        
    )

    delete_existing_records_task = PostgresOperator(
        task_id='delete_existing_records',
        postgres_conn_id='redshift_conn_id',
        sql="""
            DELETE FROM sr.service_requests
            USING sr.staging_service_requests
            WHERE sr.service_requests.unique_key = sr.staging_service_requests.unique_key;
        """,
    )

    insert_new_records_task = PostgresOperator(
        task_id='insert_new_records',
        postgres_conn_id='redshift_conn_id',
        sql="""
            INSERT INTO sr.service_requests
            SELECT * FROM sr.staging_service_requests;
        """,
    )

    truncate_staging_table_task = PostgresOperator(
        task_id='truncate_staging_table',
        postgres_conn_id='redshift_conn_id',
        sql="TRUNCATE TABLE sr.staging_service_requests;",
    )

# Set the task dependencies
extract_311_data_task >> load_to_s3_task >> is_file_in_s3_available_task >> create_staging_table_task >> transfer_s3_to_redshift_staging_task >> delete_existing_records_task >> insert_new_records_task >> truncate_staging_table_task