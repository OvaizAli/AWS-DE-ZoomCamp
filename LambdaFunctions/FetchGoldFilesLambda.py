import json
import boto3
import requests
import base64
import os

# Initialize the S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Fetch GCF URL and S3 bucket name from environment variables
    gcf_url = os.environ['GCF_URL']
    bucket_name = os.environ['GOLD_S3_BUCKET']

    try:
        # List objects in the S3 Gold bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='')  # Optionally use prefix to filter files
        files = []

        if 'Contents' in response:
            for item in response['Contents']:
                file_key = item['Key']
                # Download the file from S3
                file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
                file_content = file_obj['Body'].read()

                # Encode file content in base64 (only if needed for GCF)
                encoded_file = base64.b64encode(file_content).decode('utf-8')

                # Add the file content to the payload
                files.append({
                    'file_name': file_key,
                    'file_content': encoded_file
                })

        # If no files found, return an error
        if not files:
            return {
                'statusCode': 400,
                'body': 'No files found in the S3 Gold bucket'
            }

        # Prepare the payload to send to the Google Cloud Function
        payload = {
            "files": files  # Ensure 'files' is the expected key in the GCF
        }

        # Send POST request to Google Cloud Function
        response = requests.post(gcf_url, json=payload)

        # Log the response from GCF
        print(f"Response Status Code: {response.status_code}")
        print(f"Response Body: {response.text}")

        return {
            'statusCode': response.status_code,
            'body': response.text
        }

    except Exception as e:
        print(f"Error retrieving files from S3: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error retrieving files from S3: {str(e)}"
        }
