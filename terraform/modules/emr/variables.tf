# terraform/modules/emr/variables.tf

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID where EMR cluster will be deployed"
  type        = string
}

variable "vpc_cidr_block" {
  description = "CIDR block of the VPC"
  type        = string
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs for EMR nodes"
  type        = list(string)
}

variable "bronze_bucket_arn" { type = string }
variable "silver_bucket_arn" { type = string }
variable "gold_bucket_arn" { type = string }
variable "gold_bucket_id" { type = string }
variable "logs_bucket_arn" { type = string }
variable "logs_bucket_id" { type = string }
variable "scripts_bucket_arn" { type = string }

variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "release_label" {
  description = "EMR release label"
  type        = string
  default     = "emr-7.0.0"
}

variable "applications" {
  description = "List of applications to install on EMR cluster"
  type        = list(string)
  default     = ["Spark", "Hadoop", "Livy"]
}

variable "master_instance_type" {
  description = "Instance type for EMR master node"
  type        = string
  default     = "m5.large"
}

variable "core_instance_type" {
  description = "Instance type for EMR core nodes"
  type        = string
  default     = "m5.large"
}

variable "core_instance_count" {
  description = "Number of EMR core instances"
  type        = number
  default     = 2
}

variable "ebs_root_volume_size" {
  description = "EBS root volume size for EMR instances (GB)"
  type        = number
  default     = 40
}

variable "key_name" {
  description = "EC2 Key Pair name for EMR instances"
  type        = string
  default     = null
}

variable "termination_protection" {
  description = "Enable termination protection"
  type        = bool
  default     = false
}

variable "keep_alive" {
  description = "Keep cluster alive when no steps"
  type        = bool
  default     = true
}

variable "auto_termination_timeout_minutes" {
  description = "Auto-termination timeout in minutes (0 = disabled)"
  type        = number
  default     = 0
}

variable "spark_configuration" {
  description = "Spark configuration properties"
  type        = map(string)
  default = {
    "spark.sql.adaptive.enabled"                    = "true"
    "spark.sql.adaptive.coalescePartitions.enabled" = "true"
    "spark.sql.extensions"                          = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "spark.sql.catalog.spark_catalog"               = "org.apache.iceberg.spark.SparkSessionCatalog"
    "spark.sql.catalog.spark_catalog.type"          = "hive"
    "spark.sql.catalog.iceberg"                     = "org.apache.iceberg.spark.SparkCatalog"
    "spark.sql.catalog.iceberg.type"                = "glue"
  }
}

variable "bootstrap_actions" {
  description = "List of bootstrap actions to run on EMR cluster"
  type = list(object({
    name = string
    path = string
    args = list(string)
  }))
  default = []
}
