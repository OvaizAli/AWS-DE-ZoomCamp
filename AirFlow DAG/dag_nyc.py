from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime

# DAG Definition
dag = DAG(
    'data_processing_pipeline',
    description='Data processing pipeline from Bronze to Gold',
    schedule_interval=None,  # Set to None for manual triggering
    start_date=datetime(2024, 12, 1),
    catchup=False,
)

# Task 1: Invoke Lambda to upload data to the Bronze layer
upload_to_bronze = LambdaInvokeFunctionOperator(
    task_id='upload_to_bronze',
    function_name='FetchAndUploadParquetLambda',
    payload={'key': 'value'},  # Add any specific payload as per Lambda's requirements
    aws_conn_id='aws_default',  # Default AWS connection
    region_name='us-east-1',    # Adjust to the correct AWS region
    dag=dag
)

# Task 2: Run Glue Job for processing data from Bronze to Silver
process_bronze_to_silver = GlueJobOperator(
    task_id='process_bronze_to_silver',
    job_name='BronzeToSilverETL',
    script_location='s3://glue-scripts-nyc/bronze_to_silver_script.py',
    aws_conn_id='aws_default',  # Default AWS connection
    region_name='us-east-1',     # Adjust to the correct AWS region
    dag=dag
)

# Task 3: Run Glue Job for processing data from Silver to Gold
process_silver_to_gold = GlueJobOperator(
    task_id='process_silver_to_gold',
    job_name='SilverToGoldETL',
    script_location='s3://glue-scripts-nyc/silver_to_gold_script.py',
    aws_conn_id='aws_default',  # Default AWS connection
    region_name='us-east-1',     # Adjust to the correct AWS region
    dag=dag
)

# Set task dependencies
upload_to_bronze >> process_bronze_to_silver >> process_silver_to_gold
