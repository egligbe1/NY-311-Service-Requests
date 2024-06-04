# Lambda function to copy raw data toan intermediate S3 bucket

import boto3
import json

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Define source and destination bucket names
    source_bucket = "311-raw-bucket"
    destination_bucket = "311-intermediate-bucket"

    # Iterate through objects in the source bucket
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=source_bucket):
        for item in page.get('Contents', []):
            # Get object key (filename)
            object_key = item['Key']

            # Copy object from source to destination bucket
            copy_source = {
                'Bucket': source_bucket,
                'Key': object_key
            }
            s3_client.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=object_key)

            print(f"Copied object: {object_key}")

    return {
        'statusCode': 200,
        'body': json.dumps('Data copied successfully!')
    }