# terraform/environments/staging/outputs.tf

# Networking Outputs
output "vpc_id" {
  description = "ID of the VPC"
  value       = module.networking.vpc_id
}

output "vpc_cidr_block" {
  description = "CIDR block of the VPC"
  value       = module.networking.vpc_cidr_block
}

output "public_subnet_ids" {
  description = "IDs of the public subnets"
  value       = module.networking.public_subnet_ids
}

output "private_subnet_ids" {
  description = "IDs of the private subnets"
  value       = module.networking.private_subnet_ids
}

# Storage Outputs
output "bronze_bucket_name" {
  description = "Name of the bronze S3 bucket"
  value       = module.storage.bronze_bucket_id
}

output "silver_bucket_name" {
  description = "Name of the silver S3 bucket"
  value       = module.storage.silver_bucket_id
}

output "gold_bucket_name" {
  description = "Name of the gold S3 bucket"
  value       = module.storage.gold_bucket_id
}

output "logs_bucket_name" {
  description = "Name of the logs S3 bucket"
  value       = module.storage.logs_bucket_id
}

output "scripts_bucket_name" {
  description = "Name of the scripts S3 bucket"
  value       = module.storage.scripts_bucket_id
}

# MSK Outputs
output "msk_cluster_arn" {
  description = "ARN of the MSK cluster"
  value       = module.msk.cluster_arn
}

output "msk_cluster_name" {
  description = "Name of the MSK cluster"
  value       = module.msk.cluster_name
}

output "msk_bootstrap_brokers" {
  description = "MSK bootstrap brokers (plaintext)"
  value       = module.msk.bootstrap_brokers
}

output "msk_bootstrap_brokers_tls" {
  description = "MSK bootstrap brokers (TLS)"
  value       = module.msk.bootstrap_brokers_tls
}

output "msk_zookeeper_connect_string" {
  description = "MSK Zookeeper connection string"
  value       = module.msk.zookeeper_connect_string
}

# EMR Outputs
output "emr_cluster_id" {
  description = "ID of the EMR cluster"
  value       = module.emr.cluster_id
}

output "emr_cluster_name" {
  description = "Name of the EMR cluster"
  value       = module.emr.cluster_name
}

output "emr_master_public_dns" {
  description = "EMR master public DNS"
  value       = module.emr.master_public_dns
}

# Glue Outputs
output "glue_database_name" {
  description = "Name of the Glue database"
  value       = aws_glue_catalog_database.analytics_db.name
}

# Connection Information for Applications
output "connection_info" {
  description = "Connection information for applications"
  value = {
    environment             = var.environment
    aws_region              = var.aws_region
    kafka_bootstrap_servers = module.msk.bootstrap_brokers_tls
    s3_buckets = {
      bronze  = module.storage.bronze_bucket_id
      silver  = module.storage.silver_bucket_id
      gold    = module.storage.gold_bucket_id
      logs    = module.storage.logs_bucket_id
      scripts = module.storage.scripts_bucket_id
    }
    emr_cluster_id = module.emr.cluster_id
    glue_database  = aws_glue_catalog_database.analytics_db.name
  }
}
