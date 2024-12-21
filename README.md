# AWS-DE-ZoomCamp

## Overview

This project establishes an end-to-end data processing pipeline leveraging AWS and Google Cloud Platform services. The system ingests, processes, and transforms NYC taxi data into analysis-ready formats, storing them in BigQuery for visualization in Looker Studio.

The architecture focuses on modularity, scalability, and fault tolerance, enabling seamless data processing and real-time failure detection. Below are detailed descriptions of each system component, architecture diagrams, data flow, and deployment steps.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Features](#features)
3. [Setup Instructions](#setup-instructions)
4. [Usage](#usage)
5. [Monitoring and Debugging](#monitoring-and-debugging)
6. [File Descriptions](#file-descriptions)
7. [Future Enhancements](#future-enhancements)
8. [Author](#author)

---

## Architecture Overview

### High-Level Architecture
![AWS Cloud Architecture](Architecture%20Diagram%20-%20AWS.png)

### Data Interaction Sequence Diagram
![Data Interaction Sequence Diagram](Cloud%20Architecture%20-%20Data%20Interaction%20Sequence%20Diagram.png)

The architecture combines AWS services for ETL and Google Cloud for analytics. Key components include:

- **AWS Lambda**: Executes data fetching, transformation, and processing functions.
- **AWS Glue**: Orchestrates data transformations between `bronze`, `silver`, and `gold` S3 buckets.
- **Amazon S3**: Stores data in `bronze`, `silver`, and `gold` stages for improved processing pipelines.
- **Amazon EventBridge**: Automates pipeline triggering based on schedules.
- **AWS Step Functions**: Manages workflow state transitions.
- **Amazon SNS & CloudWatch Alarms**: Monitors the pipeline for failures and sends alerts.
- **Google Cloud Functions**: Loads processed data into BigQuery.
- **Google BigQuery and Looker Studio**: Enables querying and visualization of data insights.

---

## Features

### Data Pipeline

1. **Ingestion**:
   - Scheduled fetching of NYC taxi data via EventBridge.
   - Data stored in S3 (`bronze`).

2. **Transformation**:
   - AWS Glue ETL processes data into refined forms:
     - `Bronze` → `Silver` (cleansing).
     - `Silver` → `Gold` (aggregation).

3. **Loading**:
   - Lambda triggers a GCP function to load the `gold` data into BigQuery.

4. **Visualization**:
   - BigQuery integrates with Looker Studio for real-time dashboards.

### Monitoring & Fault Tolerance
- Dead Letter Queue (DLQ) for failure handling.
- SNS topics and CloudWatch alarms for real-time notifications.
- Step Functions for workflow monitoring.

---

## Setup Instructions

### Prerequisites

1. Install and configure:
   - AWS CLI.
   - Google Cloud CLI.
2. Create IAM roles with the necessary permissions:
   - AWS: Lambda, Glue, S3, EventBridge, SNS, SQS, Step Functions.
   - Google Cloud: Cloud Functions, BigQuery.
3. Tools:
   - [Looker Studio](https://lookerstudio.google.com/)
   - NYC taxi data API.

---

### Deployment

#### Step 1: Deploy AWS Infrastructure
1. Deploy the CloudFormation stack using the provided `CloudFormationScript.yml`:
   ```bash
   aws cloudformation deploy \
     --template-file CloudFormationScript.yml \
     --stack-name DataProcessingPipeline \
     --capabilities CAPABILITY_NAMED_IAM
## Step 2: Integrate Looker Studio

1. **Connect Looker Studio to BigQuery**.
2. **Build Interactive Dashboards**:
   - Utilize the imported NYC taxi data to create dynamic and insightful dashboards.

---

## Usage

### Data Flow
1. **Bronze**: Raw data fetched from the NYC API.
2. **Silver**: Cleansed data ready for aggregation.
3. **Gold**: Aggregated, analysis-ready data stored in S3 and BigQuery.

### Visualization
- Use Looker Studio to analyze data for:
  - NYC taxi trends.
  - Demand forecasting.
  - Seasonal patterns.

---

## Monitoring and Debugging

### Alarms and Notifications
- **CloudWatch Alarms**:
  - Monitor Lambda executions and Glue jobs.
- **SNS Notifications**:
  - Send email alerts for pipeline failures.

### Debugging Steps
1. Check logs in CloudWatch for errors in Lambda functions or Glue jobs.
2. Verify S3 bucket contents:
   - Bronze, Silver, and Gold buckets.
3. Validate successful data loading into BigQuery from the Gold S3 bucket.

---

## File Descriptions

- **`Architecture Diagram - AWS.png`**: Visual overview of the AWS cloud architecture.
- **`Cloud Architecture - Data Interaction Sequence Diagram.png`**: Sequence diagram illustrating data flow and service interactions.
- **`CloudFormationScript.yml`**: AWS CloudFormation template for pipeline deployment.
- **`CSCI5411_Term_Assignment_B00980674.pdf`**: Detailed project report explaining the pipeline and use case.

---

## Future Enhancements

1. **Real-Time Processing**:
   - Integrate Amazon Kinesis for real-time data ingestion.
2. **Enhanced Monitoring**:
   - Use AWS X-Ray for distributed tracing and debugging.
3. **Automated Testing**:
   - Implement infrastructure testing with AWS TaskCat.

---