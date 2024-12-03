import boto3
import pandas as pd
import logging
from io import BytesIO
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import sys

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3 Bucket names
silver_bucket = "silver-s3-nyc"
gold_bucket = "gold-s3-nyc"

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

# Initialize SparkContext and GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Send failed files to the Dead Letter Queue (DLQ)
def send_to_dlq(silver_key, error_message):
    sqs = boto3.client('sqs')
    dlq_url = get_dlq_url()  # Retrieve DLQ URL from Secrets Manager
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

# Function to process Silver data and write to Gold layer
def process_silver_to_gold(silver_key, gold_key):
    try:
        # Fetch the data from the Silver bucket as bytes and load it into a Pandas DataFrame
        file_obj = s3_client.get_object(Bucket=silver_bucket, Key=silver_key)
        file_content = file_obj['Body'].read()

        # Use BytesIO to simulate a file-like object that pandas can read from
        df = pd.read_parquet(BytesIO(file_content))

        # Data Cleaning: Drop rows with missing required values
        df_cleaned = df.dropna(subset=["fare_amount", "total_amount", "trip_distance"])

        # Ensure that the 'month' column is extracted from 'tpep_pickup_datetime'
        df_cleaned['month'] = pd.to_datetime(df_cleaned['tpep_pickup_datetime']).dt.month

        # Aggregations: Yearly total fare, tip amount, and trip count with month included
        df_aggregated = df_cleaned.groupby([pd.to_datetime(df_cleaned['tpep_pickup_datetime']).dt.year, 'month']).agg(
            total_trips=('VendorID', 'count'),
            total_revenue=('total_amount', 'sum'),
            total_fare=('fare_amount', 'sum'),
            total_tips=('tip_amount', 'sum'),
            total_tolls=('tolls_amount', 'sum'),
            total_congestion_surcharge=('congestion_surcharge', 'sum'),
            total_airport_fee=('Airport_fee', 'sum'),
            avg_trip_distance=('trip_distance', 'mean'),
            avg_fare_per_trip=('fare_amount', 'mean')
        ).reset_index()

        # Create Time Dimension (enhanced)
        time_dim_df = create_time_dimension(df_cleaned)

        # Create Location Dimension (Using PULocationID and DOLocationID)
        location_dim_df = create_location_dimension(df_cleaned)

        # Enhanced location-based aggregations
        location_aggregated = df_cleaned.groupby('PULocationID').agg(
            total_trips=('VendorID', 'count'),
            total_revenue=('total_amount', 'sum'),
            total_fare=('fare_amount', 'sum'),
            avg_trip_distance=('trip_distance', 'mean'),
            avg_fare_per_trip=('fare_amount', 'mean')
        ).reset_index()

        # Write consolidated yearly aggregated data to Gold layer as CSV
        s3_client.put_object(
            Body=df_aggregated.to_csv(index=False),
            Bucket=gold_bucket,
            Key=f"aggregated/yearly/total_aggregated_data.csv"
        )

        # Write individual facts and dimensions to separate CSV files for the whole year
        s3_client.put_object(
            Body=location_aggregated.to_csv(index=False),
            Bucket=gold_bucket,
            Key=f"location_aggregated/yearly/location_aggregated_data.csv"
        )
        s3_client.put_object(
            Body=time_dim_df.to_csv(index=False),
            Bucket=gold_bucket,
            Key=f"time_dim/yearly/time_dimension.csv"
        )
        s3_client.put_object(
            Body=location_dim_df.to_csv(index=False),
            Bucket=gold_bucket,
            Key=f"location_dim/yearly/location_dimension.csv"
        )

        logger.info(f"Processed and saved data from {silver_key} to Gold layer at {gold_key}")

    except Exception as e:
        logger.error(f"Error processing {silver_key}: {str(e)}")
        # Send to the Dead Letter Queue (DLQ) if processing fails
        send_to_dlq(silver_key, str(e))

# Function to create time dimension based on pickup datetime
def create_time_dimension(df):
    time_dim_df = df[['tpep_pickup_datetime']].copy()
    time_dim_df['date'] = pd.to_datetime(time_dim_df['tpep_pickup_datetime']).dt.date
    time_dim_df['day_of_week'] = pd.to_datetime(time_dim_df['tpep_pickup_datetime']).dt.dayofweek
    time_dim_df['month'] = pd.to_datetime(time_dim_df['tpep_pickup_datetime']).dt.month
    time_dim_df['year'] = pd.to_datetime(time_dim_df['tpep_pickup_datetime']).dt.year
    time_dim_df['holiday_flag'] = time_dim_df['month'].apply(lambda x: 'Holiday' if x == 12 else 'Regular')
    time_dim_df = time_dim_df[['date', 'day_of_week', 'month', 'year', 'holiday_flag']].drop_duplicates()

    return time_dim_df

# Function to create location dimension using PULocationID and DOLocationID
def create_location_dimension(df):
    location_dim_df = df[['PULocationID', 'DOLocationID']].drop_duplicates()
    # Create unique identifiers for each location
    location_dim_df['location_id'] = location_dim_df.index + 1
    return location_dim_df[['location_id', 'PULocationID', 'DOLocationID']]

# List all objects in the Silver bucket (for the year 2024)
def list_files_in_silver():
    try:
        response = s3_client.list_objects_v2(
            Bucket=silver_bucket,
            Prefix='silver/year=2024/',  # Adjust this for year and month
        )

        file_keys = []
        if 'Contents' in response:
            for item in response['Contents']:
                if "month=" in item['Key']:  # Check if it's within month-based subfolder
                    file_keys.append(item['Key'])
                    logger.info(f"Found file: {item['Key']}")
        else:
            logger.warning(f"No files found for the specified prefix: silver/year=2024/")

        return file_keys

    except Exception as e:
        logger.error(f"Error listing files in silver bucket: {str(e)}")
        return []

# Initialize the S3 client
s3_client = boto3.client('s3')

# Retrieve list of all files in the silver bucket (for the year 2024)
files_to_process = list_files_in_silver()

# Process each file
for silver_key in files_to_process:
    if silver_key.endswith(".parquet"):  # Ensure we process only parquet files
        # Generate gold_key for year-based storage
        gold_key = f"year=2024/{silver_key.replace('silver/', '')}"  # Adjust if needed

        # Call the process function
        process_silver_to_gold(silver_key, gold_key)

# Complete the job
logger.info("Job completed.")
job = Job(glueContext)  # Now glueContext is initialized
job.init(args['JOB_NAME'], {})  
job.commit()
