import boto3
import json
import pandas as pd
from io import StringIO
import logging
import botocore

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """Processes JSON data from S3, converts it to CSV, and uploads to target bucket, then deletes the source object."""
    
    # Define source and destination bucket names
    source_bucket = "311-intermediate-bucket"
    target_bucket = "311-cleaned-bucket"

    # Create a paginator to iterate through objects in the source bucket
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=source_bucket):
        for item in page.get('Contents', []):
            # Get the object key (filename)
            object_key = item['Key']
            
            # Define the target filename by replacing ".json" with ".csv"
            target_file_name = object_key.replace('.json', '.csv')
            
            # Wait until the object exists in the bucket
            waiter = s3_client.get_waiter('object_exists')
            try:
                waiter.wait(Bucket=source_bucket, Key=object_key)
            except botocore.exceptions.ClientError as e:
                logging.error(f"Error waiting for object in S3: {e}")
                return {
                    'statusCode': 500,
                    'body': json.dumps('Error waiting for S3 object.')
                }
                
            # Download JSON data from the source bucket
            try:
                response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
                json_data = response['Body'].read().decode('utf-8')
            except botocore.exceptions.ClientError as e:
                logging.error(f"Error downloading object from S3: {e}")
                return {
                    'statusCode': 500,
                    'body': json.dumps('Error downloading S3 object.')
                }
            
            # Load the JSON data into a pandas DataFrame
            try:
                df = pd.read_json(StringIO(json_data))
            except (json.JSONDecodeError, ValueError) as e:
                logging.error(f"Error parsing JSON data: {e}")
                return {
                    'statusCode': 400,
                    'body': json.dumps('Invalid JSON data format.')
                }
                
            # Specify the columns to retain
            columns_to_keep = [
                "unique_key", "created_date", "closed_date", "agency", "agency_name", 
                "complaint_type", "descriptor", "location_type", "incident_address", 
                "city", "borough", "incident_zip", "landmark", "status", 
                "resolution_description", "resolution_action_updated_date", 
                "open_data_channel_type", "latitude", "longitude"
            ]
            
            # Filter DataFrame to keep only the specified columns
            try:
                df = df[columns_to_keep]
            except KeyError as e:
                logging.error(f"Error filtering DataFrame columns: {e}")
                return {
                    'statusCode': 400,
                    'body': json.dumps('Invalid column names.')
                }
                
            # Convert and format data types
            try:
                df['created_date'] = pd.to_datetime(df['created_date']).dt.strftime('%Y-%m-%d %H:%M:%S')
                df['closed_date'] = pd.to_datetime(df['closed_date']).dt.strftime('%Y-%m-%d %H:%M:%S')
                df['resolution_action_updated_date'] = pd.to_datetime(df['resolution_action_updated_date']).dt.strftime('%Y-%m-%d %H:%M:%S')
                df['incident_zip'] = df['incident_zip'].fillna(0).astype(int)
            except (ValueError, KeyError) as e:
                logging.error(f"Error processing data types: {e}")
                return {
                    'statusCode': 400,
                    'body': json.dumps('Error processing data types.')
                }
            
            # Convert DataFrame to CSV
            try:
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)
                csv_data = csv_buffer.getvalue()
            except Exception as e:
                logging.error(f"Error converting DataFrame to CSV: {e}")
                return {
                    'statusCode': 500,
                    'body': json.dumps('Error converting data to CSV.')
                }
            
            # Upload CSV to the target bucket
            try:
                s3_client.put_object(Bucket=target_bucket, Key=target_file_name, Body=csv_data)
            except botocore.exceptions.ClientError as e:
                logging.error(f"Error uploading CSV to S3: {e}")
                return {
                    'statusCode': 500,
                    'body': json.dumps('Error uploading CSV to S3.')
                }
            
            # Delete the JSON object from the source bucket
            try:
                s3_client.delete_object(Bucket=source_bucket, Key=object_key)
                logging.info(f"Successfully deleted {object_key} from {source_bucket}")
            except botocore.exceptions.ClientError as e:
                logging.error(f"Error deleting object from S3: {e}")
                return {
                    'statusCode': 500,
                    'body': json.dumps('Error deleting object from S3.')
                }
    
    return {
        'statusCode': 200,
        'body': json.dumps('CSV file successfully created, uploaded, and source object deleted.')
    }

