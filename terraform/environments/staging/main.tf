# terraform/environments/staging/main.tf

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.1"
    }
  }

  # Uncomment and configure for remote state
  # backend "s3" {
  #   bucket = "your-terraform-state-bucket"
  #   key    = "staging/terraform.tfstate"
  #   region = "us-west-2"
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = merge(var.common_tags, {
      Environment = var.environment
      ManagedBy   = "terraform"
    })
  }
}

# Data sources
data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

# Networking Module
module "networking" {
  source = "../../modules/networking"

  project_name             = var.project_name
  environment              = var.environment
  vpc_cidr                 = var.vpc_cidr
  availability_zones_count = var.availability_zones_count
  create_nat_gateways      = var.create_nat_gateways
  common_tags              = var.common_tags
}

# Storage Module
module "storage" {
  source = "../../modules/storage"

  project_name                      = var.project_name
  environment                       = var.environment
  common_tags                       = var.common_tags
  enable_versioning                 = var.enable_versioning
  enable_encryption                 = var.enable_encryption
  enable_lifecycle_management       = var.enable_lifecycle_management
  bronze_transition_to_ia_days      = var.bronze_transition_to_ia_days
  bronze_transition_to_glacier_days = var.bronze_transition_to_glacier_days
  bronze_expiration_days            = var.bronze_expiration_days
  silver_transition_to_ia_days      = var.silver_transition_to_ia_days
  silver_transition_to_glacier_days = var.silver_transition_to_glacier_days
}

# MSK Module
module "msk" {
  source = "../../modules/msk"

  project_name       = var.project_name
  environment        = var.environment
  vpc_id             = module.networking.vpc_id
  vpc_cidr_block     = module.networking.vpc_cidr_block
  private_subnet_ids = module.networking.private_subnet_ids
  logs_bucket_id     = module.storage.logs_bucket_id
  common_tags        = var.common_tags

  # MSK Configuration
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.kafka_number_of_broker_nodes
  instance_type          = var.kafka_instance_type
  ebs_volume_size        = var.kafka_ebs_volume_size

  # Security & Logging
  enable_encryption        = var.enable_encryption
  client_broker_encryption = var.kafka_client_broker_encryption
  enable_logging           = var.enable_monitoring
  log_retention_days       = var.log_retention_days
  log_retention_hours      = var.kafka_log_retention_hours
}

# EMR Module
module "emr" {
  source = "../../modules/emr"

  project_name       = var.project_name
  environment        = var.environment
  vpc_id             = module.networking.vpc_id
  vpc_cidr_block     = module.networking.vpc_cidr_block
  private_subnet_ids = module.networking.private_subnet_ids

  # S3 Bucket ARNs
  bronze_bucket_arn  = module.storage.bronze_bucket_arn
  silver_bucket_arn  = module.storage.silver_bucket_arn
  gold_bucket_arn    = module.storage.gold_bucket_arn
  gold_bucket_id     = module.storage.gold_bucket_id
  logs_bucket_arn    = module.storage.logs_bucket_arn
  logs_bucket_id     = module.storage.logs_bucket_id
  scripts_bucket_arn = module.storage.scripts_bucket_arn

  common_tags = var.common_tags

  # EMR Configuration
  release_label        = var.emr_release_label
  master_instance_type = var.emr_master_instance_type
  core_instance_type   = var.emr_core_instance_type
  core_instance_count  = var.emr_core_instance_count
  ebs_root_volume_size = var.emr_ebs_root_volume_size
  key_name             = var.emr_key_name

  # Cost optimization for staging
  termination_protection           = false
  keep_alive                       = var.emr_keep_alive
  auto_termination_timeout_minutes = var.emr_auto_termination_timeout_minutes
}

# Glue Data Catalog
resource "aws_glue_catalog_database" "analytics_db" {
  name        = "${replace(var.project_name, "-", "_")}_${var.environment}"
  description = "Analytics database for e-commerce streaming project - ${var.environment}"

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-glue-db-${var.environment}"
    Environment = var.environment
    Service     = "glue"
  })
}
