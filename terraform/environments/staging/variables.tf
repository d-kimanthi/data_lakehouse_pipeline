# terraform/environments/staging/variables.tf

# Import shared variables
variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "ecommerce-streaming-analytics"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "staging"
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-west-2"
}

variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default = {
    Project   = "ecommerce-streaming-analytics"
    ManagedBy = "terraform"
    Owner     = "data-engineering-team"
  }
}

# Networking Configuration
variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones_count" {
  description = "Number of availability zones to use"
  type        = number
  default     = 3
}

variable "create_nat_gateways" {
  description = "Whether to create NAT gateways (cost optimization)"
  type        = bool
  default     = true # Set to false for cost savings in staging
}

# Storage Configuration
variable "enable_versioning" {
  description = "Enable S3 bucket versioning"
  type        = bool
  default     = true
}

variable "enable_encryption" {
  description = "Enable encryption at rest for supported services"
  type        = bool
  default     = true
}

variable "enable_lifecycle_management" {
  description = "Enable S3 lifecycle management"
  type        = bool
  default     = true
}

variable "bronze_transition_to_ia_days" {
  description = "Days before transitioning bronze data to IA"
  type        = number
  default     = 7 # Shorter for staging
}

variable "bronze_transition_to_glacier_days" {
  description = "Days before transitioning bronze data to Glacier"
  type        = number
  default     = 30 # Shorter for staging
}

variable "bronze_expiration_days" {
  description = "Days before expiring bronze data"
  type        = number
  default     = 90 # Shorter retention for staging
}

variable "silver_transition_to_ia_days" {
  description = "Days before transitioning silver data to IA"
  type        = number
  default     = 14
}

variable "silver_transition_to_glacier_days" {
  description = "Days before transitioning silver data to Glacier"
  type        = number
  default     = 60
}

# Kafka/MSK Configuration
variable "kafka_version" {
  description = "Apache Kafka version"
  type        = string
  default     = "3.5.1"
}

variable "kafka_number_of_broker_nodes" {
  description = "Number of broker nodes in MSK cluster"
  type        = number
  default     = 3
}

variable "kafka_instance_type" {
  description = "Instance type for Kafka brokers (staging optimized)"
  type        = string
  default     = "kafka.t3.small" # Cost-optimized for staging
}

variable "kafka_ebs_volume_size" {
  description = "EBS volume size for Kafka brokers (GB)"
  type        = number
  default     = 100
}

variable "kafka_client_broker_encryption" {
  description = "Encryption setting for client-broker communication"
  type        = string
  default     = "TLS"
}

variable "kafka_log_retention_hours" {
  description = "Kafka log retention period in hours"
  type        = number
  default     = 168 # 7 days
}

# EMR Configuration  
variable "emr_release_label" {
  description = "EMR release label"
  type        = string
  default     = "emr-7.0.0"
}

variable "emr_master_instance_type" {
  description = "Instance type for EMR master node (staging optimized)"
  type        = string
  default     = "m5.large" # Smaller for staging
}

variable "emr_core_instance_type" {
  description = "Instance type for EMR core nodes (staging optimized)"
  type        = string
  default     = "m5.large" # Smaller for staging
}

variable "emr_core_instance_count" {
  description = "Number of EMR core instances"
  type        = number
  default     = 2
}

variable "emr_ebs_root_volume_size" {
  description = "EBS root volume size for EMR instances (GB)"
  type        = number
  default     = 40
}

variable "emr_key_name" {
  description = "EC2 Key Pair name for EMR instances"
  type        = string
  default     = null
}

variable "emr_keep_alive" {
  description = "Keep EMR cluster alive when no steps"
  type        = bool
  default     = false # Auto-terminate for cost savings
}

variable "emr_auto_termination_timeout_minutes" {
  description = "Auto-termination timeout in minutes for cost optimization"
  type        = number
  default     = 240 # 4 hours - good for demos
}

# Monitoring Configuration
variable "enable_monitoring" {
  description = "Enable CloudWatch monitoring and logging"
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "CloudWatch log retention period in days"
  type        = number
  default     = 14 # Shorter retention for staging
}
