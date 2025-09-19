#!/bin/bash

echo "Deploying E-commerce Analytics Platform to AWS..."

# Check if Terraform is installed
command -v terraform >/dev/null 2>&1 || { echo "Terraform is required but not installed. Aborting." >&2; exit 1; }

# Check if AWS CLI is configured
aws sts get-caller-identity > /dev/null 2>&1 || { echo "AWS CLI is not configured. Please run 'aws configure' first." >&2; exit 1; }

# Load environment variables
source .env

# Validate required environment variables
if [ -z "$AWS_REGION" ] || [ -z "$S3_BUCKET" ]; then
    echo "Please set AWS_REGION and S3_BUCKET in your .env file"
    exit 1
fi

# Initialize Terraform
echo "Initializing Terraform..."
cd terraform/aws
terraform init

# Plan the deployment
echo "Planning Terraform deployment..."
terraform plan \
    -var="aws_region=${AWS_REGION}" \
    -var="project_name=ecommerce-streaming-analytics" \
    -var="environment=production" \
    -var="s3_bucket_name=${S3_BUCKET}" \
    -out=tfplan

# Apply the plan
read -p "Do you want to apply this plan? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Applying Terraform plan..."
    terraform apply tfplan
    
    # Get outputs
    echo "Deployment complete! Getting resource information..."
    terraform output
    
    # Save important outputs to environment file
    echo "Updating environment file with deployment outputs..."
    MSK_BOOTSTRAP_SERVERS=$(terraform output -raw msk_bootstrap_servers)
    EMR_CLUSTER_ID=$(terraform output -raw emr_cluster_id)
    GLUE_DATABASE_NAME=$(terraform output -raw glue_database_name)
    
    cat >> ../../.env << EOF

# AWS Deployment Outputs
MSK_BOOTSTRAP_SERVERS=$MSK_BOOTSTRAP_SERVERS
EMR_CLUSTER_ID=$EMR_CLUSTER_ID
GLUE_DATABASE_NAME=$GLUE_DATABASE_NAME
EOF
    
    echo "Deployment complete! Check the AWS console for your resources."
    echo "MSK Bootstrap Servers: $MSK_BOOTSTRAP_SERVERS"
    echo "EMR Cluster ID: $EMR_CLUSTER_ID"
    echo "Glue Database: $GLUE_DATABASE_NAME"
else
    echo "Deployment cancelled."
fi

cd ../..