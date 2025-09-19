# terraform/modules/storage/variables.tf

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "enable_versioning" {
  description = "Enable S3 bucket versioning"
  type        = bool
  default     = true
}

variable "enable_encryption" {
  description = "Enable S3 bucket encryption"
  type        = bool
  default     = true
}

variable "enable_lifecycle_management" {
  description = "Enable S3 lifecycle management"
  type        = bool
  default     = true
}

# Bronze layer lifecycle settings
variable "bronze_transition_to_ia_days" {
  description = "Days before transitioning bronze data to IA"
  type        = number
  default     = 30
}

variable "bronze_transition_to_glacier_days" {
  description = "Days before transitioning bronze data to Glacier"
  type        = number
  default     = 90
}

variable "bronze_expiration_days" {
  description = "Days before expiring bronze data (0 = never expire)"
  type        = number
  default     = 365
}

# Silver layer lifecycle settings
variable "silver_transition_to_ia_days" {
  description = "Days before transitioning silver data to IA"
  type        = number
  default     = 60
}

variable "silver_transition_to_glacier_days" {
  description = "Days before transitioning silver data to Glacier"
  type        = number
  default     = 180
}
