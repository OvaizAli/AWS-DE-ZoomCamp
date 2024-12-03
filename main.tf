provider "aws" {
  region = "us-east-1"
}

resource "aws_vpc" "data_processing_vpc" {
  cidr_block = "10.0.0.0/16"
  enable_dns_support = true
  enable_dns_hostnames = true

  tags = {
    Name = "DataProcessingVPC"
  }
}

resource "aws_security_group" "lambda_security_group" {
  name        = "LambdaSecurityGroup"
  description = "Allow Lambda to access S3 and NAT Gateway"
  vpc_id      = aws_vpc.data_processing_vpc.id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_subnet" "public_subnet" {
  vpc_id            = aws_vpc.data_processing_vpc.id
  cidr_block        = "10.0.1.0/24"
  map_public_ip_on_launch = true
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = {
    Name = "PublicSubnet"
  }
}

resource "aws_subnet" "private_subnet" {
  vpc_id            = aws_vpc.data_processing_vpc.id
  cidr_block        = "10.0.2.0/24"
  map_public_ip_on_launch = false
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = {
    Name = "PrivateSubnet"
  }
}

resource "aws_internet_gateway" "internet_gateway" {
  vpc_id = aws_vpc.data_processing_vpc.id

  tags = {
    Name = "DataProcessingIGW"
  }
}

resource "aws_nat_gateway" "nat_gateway" {
  allocation_id = aws_eip.elastic_ip.id
  subnet_id     = aws_subnet.public_subnet.id
}

resource "aws_eip" "elastic_ip" {
  vpc = true
}

resource "aws_route_table" "public_route_table" {
  vpc_id = aws_vpc.data_processing_vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.internet_gateway.id
  }

  tags = {
    Name = "PublicRouteTable"
  }
}

resource "aws_route_table" "private_route_table" {
  vpc_id = aws_vpc.data_processing_vpc.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.nat_gateway.id
  }

  tags = {
    Name = "PrivateRouteTable"
  }
}

resource "aws_vpc_endpoint" "s3_vpc_endpoint" {
  service_name = "com.amazonaws.${var.region}.s3"
  vpc_id       = aws_vpc.data_processing_vpc.id
  route_table_ids = [aws_route_table.private_route_table.id]
}

resource "aws_vpc_endpoint" "secrets_manager_vpc_endpoint" {
  service_name      = "com.amazonaws.${var.region}.secretsmanager"
  vpc_id            = aws_vpc.data_processing_vpc.id
  subnet_ids        = [aws_subnet.private_subnet.id]
  security_group_ids = [aws_security_group.lambda_security_group.id]
  vpc_endpoint_type = "Interface"
}

resource "aws_s3_bucket" "bronze_s3_bucket" {
  bucket = "bronze-s3-nyc"
  versioning {
    enabled = true
  }
  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }
  lifecycle {
    rule {
      enabled = true
      transition {
        storage_class = "INTELLIGENT_TIERING"
        transition_in_days = 30
      }
      transition {
        storage_class = "GLACIER"
        transition_in_days = 365
      }
      expiration {
        days = 730
      }
    }
  }
  public_access_block {
    block_public_acls        = true
    block_public_policy      = true
    ignore_public_acls       = true
    restrict_public_buckets  = true
  }
}

resource "aws_lambda_function" "fetch_and_upload_parquet_lambda" {
  function_name = "FetchAndUploadParquetLambda"
  handler       = "index.lambda_handler"
  runtime       = "python3.9"
  timeout       = 120
  memory_size   = 512
  vpc_config {
    subnet_ids         = [aws_subnet.private_subnet.id]
    security_group_ids = [aws_security_group.lambda_security_group.id]
  }

  environment {
    variables = {
      S3_BUCKET_NAME = aws_s3_bucket.bronze_s3_bucket.bucket
      DLQ_URL        = aws_secretsmanager_secret.dlq_secret.secret_string
    }
  }

  role = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/LabRole"
}

resource "aws_secretsmanager_secret" "dlq_secret" {
  name = "DLQURL"
  secret_string = jsonencode({
    DLQ_URL = aws_sqs_queue.lambda_dlq.id
  })
}

resource "aws_sqs_queue" "lambda_dlq" {
  name = "LambdaDLQ"
}

resource "aws_glue_job" "bronze_to_silver_glue_job" {
  name     = "BronzeToSilverETL"
  role     = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/LabRole"
  glue_version = "3.0"
  command {
    name            = "glueetl"
    script_location = "s3://glue-scripts-nyc/bronze_to_silver_script.py"
    python_version  = "3"
  }
  default_arguments = {
    "--TempDir"                        = "s3://${aws_s3_bucket.silver_s3_bucket.bucket}/temp/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--DLQ_URL"                        = aws_secretsmanager_secret.dlq_secret.secret_string
    "--enable-job-bookmarks"            = "true"
    "--enable-metrics"                  = "true"
  }
  max_capacity = 10
  timeout      = 60
}

resource "aws_glue_job" "silver_to_gold_glue_job" {
  name     = "SilverToGoldETL"
  role     = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/LabRole"
  glue_version = "3.0"
  command {
    name            = "glueetl"
    script_location = "s3://glue-scripts-nyc/silver_to_gold_script.py"
    python_version  = "3"
  }
  default_arguments = {
    "--TempDir"                        = "s3://${aws_s3_bucket.gold_s3_bucket.bucket}/temp/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--DLQ_URL"                        = aws_secretsmanager_secret.dlq_secret.secret_string
    "--enable-job-bookmarks"            = "true"
    "--enable-metrics"                  = "true"
  }
  max_capacity = 10
  timeout      = 120
}

output "bronze_s3_bucket_name" {
  value = aws_s3_bucket.bronze_s3_bucket.bucket
}

output "fetch_and_upload_parquet_lambda_arn" {
  value = aws_lambda_function.fetch_and_upload_parquet_lambda.arn
}

output "bronze_to_silver_glue_job_name" {
  value = aws_glue_job.bronze_to_silver_glue_job.name
}

output "silver_to_gold_glue_job_name" {
  value = aws_glue_job.silver_to_gold_glue_job.name
}

output "s3_vpc_endpoint_id" {
  value = aws_vpc_endpoint.s3_vpc_endpoint.id
}

output "secrets_manager_vpc_endpoint_id" {
  value = aws_vpc_endpoint.secrets_manager_vpc_endpoint.id
}
