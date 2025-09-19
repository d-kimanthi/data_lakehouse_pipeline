# terraform/modules/storage/outputs.tf

output "bronze_bucket_id" {
  description = "ID of the bronze bucket"
  value       = aws_s3_bucket.bronze.id
}

output "bronze_bucket_arn" {
  description = "ARN of the bronze bucket"
  value       = aws_s3_bucket.bronze.arn
}

output "silver_bucket_id" {
  description = "ID of the silver bucket"
  value       = aws_s3_bucket.silver.id
}

output "silver_bucket_arn" {
  description = "ARN of the silver bucket"
  value       = aws_s3_bucket.silver.arn
}

output "gold_bucket_id" {
  description = "ID of the gold bucket"
  value       = aws_s3_bucket.gold.id
}

output "gold_bucket_arn" {
  description = "ARN of the gold bucket"
  value       = aws_s3_bucket.gold.arn
}

output "logs_bucket_id" {
  description = "ID of the logs bucket"
  value       = aws_s3_bucket.logs.id
}

output "logs_bucket_arn" {
  description = "ARN of the logs bucket"
  value       = aws_s3_bucket.logs.arn
}

output "scripts_bucket_id" {
  description = "ID of the scripts bucket"
  value       = aws_s3_bucket.scripts.id
}

output "scripts_bucket_arn" {
  description = "ARN of the scripts bucket"
  value       = aws_s3_bucket.scripts.arn
}

output "bucket_suffix" {
  description = "Random suffix used for bucket naming"
  value       = random_id.bucket_suffix.hex
}
