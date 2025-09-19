# terraform/modules/msk/variables.tf

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID where MSK cluster will be deployed"
  type        = string
}

variable "vpc_cidr_block" {
  description = "CIDR block of the VPC"
  type        = string
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs for MSK brokers"
  type        = list(string)
}

variable "logs_bucket_id" {
  description = "S3 bucket ID for storing MSK logs"
  type        = string
  default     = ""
}

variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}

# MSK Configuration
variable "kafka_version" {
  description = "Apache Kafka version"
  type        = string
  default     = "3.5.1"
}

variable "number_of_broker_nodes" {
  description = "Number of broker nodes in the MSK cluster"
  type        = number
  default     = 3
}

variable "instance_type" {
  description = "Instance type for MSK brokers"
  type        = string
  default     = "kafka.t3.small"
}

variable "ebs_volume_size" {
  description = "EBS volume size for each broker (GB)"
  type        = number
  default     = 100
}

# Encryption Configuration
variable "enable_encryption" {
  description = "Enable encryption for MSK cluster"
  type        = bool
  default     = true
}

variable "client_broker_encryption" {
  description = "Encryption setting for client-broker communication"
  type        = string
  default     = "TLS"
  validation {
    condition     = contains(["TLS", "TLS_PLAINTEXT", "PLAINTEXT"], var.client_broker_encryption)
    error_message = "Valid values are TLS, TLS_PLAINTEXT, or PLAINTEXT."
  }
}

variable "in_cluster_encryption" {
  description = "Enable encryption for inter-cluster communication"
  type        = bool
  default     = true
}

# Authentication Configuration
variable "enable_client_authentication" {
  description = "Enable client authentication for MSK cluster"
  type        = bool
  default     = false
}

variable "certificate_authority_arns" {
  description = "List of ACM Certificate Authority ARNs"
  type        = list(string)
  default     = []
}

# Logging Configuration
variable "enable_logging" {
  description = "Enable logging for MSK cluster"
  type        = bool
  default     = true
}

variable "enable_cloudwatch_logs" {
  description = "Enable CloudWatch logs"
  type        = bool
  default     = true
}

variable "enable_s3_logs" {
  description = "Enable S3 logs"
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "CloudWatch log retention period in days"
  type        = number
  default     = 30
}

# Kafka Configuration
variable "log_retention_hours" {
  description = "Kafka log retention period in hours"
  type        = number
  default     = 168 # 7 days
}

variable "log_retention_bytes" {
  description = "Kafka log retention size in bytes (-1 for unlimited)"
  type        = number
  default     = -1
}
