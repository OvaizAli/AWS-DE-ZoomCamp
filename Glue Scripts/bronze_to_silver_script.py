import boto3
import pandas as pd
from io import BytesIO
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import sys
import logging

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize the Spark and Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# S3 Bucket names
bronze_bucket = "bronze-s3-nyc"  
silver_bucket = "silver-s3-nyc"

# Initialize the S3 client
s3_client = boto3.client('s3')

# Get job name from arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Retrieve DLQ URL from Secrets Manager
def get_dlq_url():
    secret_name = "DLQURL"
    region_name = "us-east-1"  # Specify the correct region

    # Create a Secrets Manager client
    client = boto3.client("secretsmanager", region_name=region_name)

    try:
        # Retrieve the secret value
        response = client.get_secret_value(SecretId=secret_name)
        secret = response["SecretString"]
        dlq_url = secret["DLQ_URL"]
        return dlq_url
    except Exception as e:
        logger.error(f"Error retrieving DLQ URL from Secrets Manager: {str(e)}")
        raise

# Send failed files to the Dead Letter Queue (DLQ)
def send_to_dlq(bronze_key, error_message):
    sqs = boto3.client('sqs')
    dlq_url = get_dlq_url()  # Retrieve DLQ URL from Secrets Manager
    message = {
        'bronze_key': bronze_key,
        'error_message': error_message
    }
    try:
        sqs.send_message(
            QueueUrl=dlq_url,
            MessageBody=str(message)
        )
        logger.info(f"Error details sent to DLQ for {bronze_key}")
    except Exception as e:
        logger.error(f"Error sending message to DLQ: {str(e)}")

# Function to process a file from Bronze and store it in Silver
def process_bronze_to_silver(bronze_key, silver_key):
    try:
        # Fetch the data from the Bronze bucket as a pandas DataFrame
        response = s3_client.get_object(Bucket=bronze_bucket, Key=bronze_key)
        raw_data = response['Body'].read()

        # Convert raw data to a pandas DataFrame
        df = pd.read_parquet(BytesIO(raw_data))

        # Perform data cleaning or transformation (e.g., drop missing values)
        df_cleaned = df.dropna()

        # Write the processed DataFrame to the Silver bucket
        buffer = BytesIO()
        df_cleaned.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        # Upload the cleaned data to Silver bucket
        s3_client.put_object(
            Bucket=silver_bucket,
            Key=silver_key,
            Body=buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        logger.info(f"Processed {bronze_key} and saved to {silver_key}")
    except Exception as e:
        logger.error(f"Error processing {bronze_key}: {str(e)}")
        send_to_dlq(bronze_key, str(e))

# List all objects in the 'bronze' bucket for the year 2024
def list_files_in_bronze():
    try:
        response = s3_client.list_objects_v2(
            Bucket=bronze_bucket,
            Prefix='bronze/year=2024/',  # Adjust this for year and month
        )

        file_keys = []
        if 'Contents' in response:
            for item in response['Contents']:
                if "month=" in item['Key']:  # Check if it's within month-based subfolder
                    file_keys.append(item['Key'])
                    logger.info(f"Found file: {item['Key']}")  # Debugging line to see what files are found
        else:
            logger.warning(f"No files found for the specified prefix: bronze/year=2024/")

        return file_keys

    except Exception as e:
        logger.error(f"Error listing files in bronze bucket: {str(e)}")
        return []

# Retrieve list of all files in the bronze bucket (for year 2024)
files_to_process = list_files_in_bronze()

# Process each file
for bronze_key in files_to_process:
    if bronze_key.endswith(".parquet"):  # Ensure we process only parquet files
        silver_key = bronze_key.replace("bronze/", "silver/")
        process_bronze_to_silver(bronze_key, silver_key)

# Complete the job
logger.info("Job completed.")
job = Job(glueContext)  # Using glueContext to initialize the job
job.init(args['JOB_NAME'], {})  
job.commit()
