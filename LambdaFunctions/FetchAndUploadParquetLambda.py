import boto3
import requests
import os
import logging
from botocore.exceptions import ClientError

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    s3_bucket_name = os.environ['S3_BUCKET_NAME']
    s3_client = boto3.client('s3')
    dlq_url = os.environ['DLQ_URL']

    for month in range(1, 9):  # Adjust range as needed
        month_str = f"{month:02d}"
        file_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{month_str}.parquet"
        s3_key = f"bronze/year=2024/month={month_str}/yellow_tripdata_2024-{month_str}.parquet"
        
        # Check if file already exists in S3
        try:
            s3_client.head_object(Bucket=s3_bucket_name, Key=s3_key)
            logger.info(f"File already exists: {s3_key}. Skipping download.")
            continue
        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                logger.error(f"Unexpected error while checking file existence: {str(e)}")
                raise

        # Download file and upload to S3
        try:
            response = requests.get(file_url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Upload file to S3
            s3_client.put_object(
                Bucket=s3_bucket_name,
                Key=s3_key,
                Body=response.content,
                ContentType='application/octet-stream'
            )
            logger.info(f"Successfully uploaded file to {s3_key}")
        

        except requests.exceptions.RequestException as req_err:
            logger.error(f"Error downloading file from {file_url}: {str(req_err)}")
            send_to_dlq(dlq_url, str(req_err))
        except ClientError as s3_err:
            logger.error(f"Error uploading file to S3: {str(s3_err)}")
            send_to_dlq(dlq_url, str(s3_err))

    return {"statusCode": 200, "body": "Upload completed."}

def send_to_dlq(dlq_url, error_message):
    sqs = boto3.client('sqs')
    message = {
        'errorMessage': error_message
    }
    sqs.send_message(
        QueueUrl=dlq_url,
        MessageBody=str(message)
    )
