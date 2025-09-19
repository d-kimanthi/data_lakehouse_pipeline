# terraform/shared/common-variables.tf

# Project Configuration
variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "ecommerce-streaming-analytics"
}

variable "environment" {
  description = "Environment name (local, staging, production)"
  type        = string
  validation {
    condition     = contains(["local", "staging", "production"], var.environment)
    error_message = "Environment must be local, staging, or production."
  }
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-west-2"
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

# Common Tags
variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default = {
    Project   = "ecommerce-streaming-analytics"
    ManagedBy = "terraform"
    Owner     = "data-engineering-team"
  }
}

# Cost Management
variable "enable_cost_allocation_tags" {
  description = "Enable cost allocation tags for cost tracking"
  type        = bool
  default     = true
}

# Security Configuration
variable "enable_encryption" {
  description = "Enable encryption at rest for supported services"
  type        = bool
  default     = true
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
  default     = 30
}
