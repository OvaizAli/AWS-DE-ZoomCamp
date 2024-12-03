import boto3
import pandas as pd
import logging
from io import BytesIO
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import DataFrame
import sys

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3 Bucket names
silver_bucket = "silver-s3-nyc"

# Redshift Connection
redshift_connection = "jdbc:redshift://redshift-cluster-url:5439/dbname"
redshift_table = "processed_data"

# Get job name from arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Retrieve DLQ URL from Secrets Manager
def get_dlq_url():
    secret_name = "DLQURL"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    client = boto3.client("secretsmanager", region_name=region_name)

    try:
        # Retrieve the secret value
        response = client.get_secret_value(SecretId=secret_name)
        secret = eval(response["SecretString"])
        return secret["DLQ_URL"]
    except Exception as e:
        logger.error(f"Error retrieving DLQ URL from Secrets Manager: {str(e)}")
        raise

# Send failed files to the Dead Letter Queue (DLQ)
def send_to_dlq(silver_key, error_message):
    sqs = boto3.client('sqs')
    dlq_url = get_dlq_url()
    message = {
        'silver_key': silver_key,
        'error_message': error_message
    }
    try:
        sqs.send_message(
            QueueUrl=dlq_url,
            MessageBody=str(message)
        )
        logger.info(f"Error details sent to DLQ for {silver_key}")
    except Exception as e:
        logger.error(f"Error sending message to DLQ: {str(e)}")

# Initialize SparkContext and GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], {})

# Process data from Silver to Redshift
def process_silver_to_redshift(silver_key):
    try:
        logger.info(f"Processing file: {silver_key}")
        # Read data from Silver bucket into Spark DataFrame
        silver_df = spark.read.parquet(f"s3://{silver_bucket}/{silver_key}")

        # Perform cleaning and transformations
        cleaned_df = silver_df.dropna(subset=["fare_amount", "total_amount", "trip_distance"])

        # Write transformed data to Redshift
        cleaned_df.write \
            .format("jdbc") \
            .option("url", redshift_connection) \
            .option("dbtable", redshift_table) \
            .option("user", "your_username") \
            .option("password", "your_password") \
            .option("driver", "com.amazon.redshift.jdbc42.Driver") \
            .mode("append") \
            .save()

        logger.info(f"Successfully processed and loaded data from {silver_key} into Redshift")

    except Exception as e:
        logger.error(f"Error processing {silver_key}: {str(e)}")
        send_to_dlq(silver_key, str(e))

# List all files in the Silver bucket (for the year 2024)
def list_files_in_silver():
    try:
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(
            Bucket=silver_bucket,
            Prefix='silver/year=2024/'
        )

        file_keys = []
        if 'Contents' in response:
            for item in response['Contents']:
                if "month=" in item['Key']:
                    file_keys.append(item['Key'])
                    logger.info(f"Found file: {item['Key']}")
        else:
            logger.warning("No files found for the specified prefix.")
        return file_keys
    except Exception as e:
        logger.error(f"Error listing files in silver bucket: {str(e)}")
        return []

# Retrieve the list of files to process
files_to_process = list_files_in_silver()

# Process each file
for silver_key in files_to_process:
    if silver_key.endswith(".parquet"):
        process_silver_to_redshift(silver_key)

# Commit the Glue Job
logger.info("Job completed.")
job.commit()
