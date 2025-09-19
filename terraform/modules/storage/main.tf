# terraform/modules/storage/main.tf

# Random ID for unique bucket naming
resource "random_id" "bucket_suffix" {
  byte_length = 8
}

# Data Lake S3 Buckets - Bronze Layer
resource "aws_s3_bucket" "bronze" {
  bucket = "${var.project_name}-${var.environment}-bronze-${random_id.bucket_suffix.hex}"

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-${var.environment}-bronze"
    Environment = var.environment
    Layer       = "bronze"
    Purpose     = "raw-data"
  })
}

# Data Lake S3 Buckets - Silver Layer
resource "aws_s3_bucket" "silver" {
  bucket = "${var.project_name}-${var.environment}-silver-${random_id.bucket_suffix.hex}"

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-${var.environment}-silver"
    Environment = var.environment
    Layer       = "silver"
    Purpose     = "cleaned-data"
  })
}

# Data Lake S3 Buckets - Gold Layer
resource "aws_s3_bucket" "gold" {
  bucket = "${var.project_name}-${var.environment}-gold-${random_id.bucket_suffix.hex}"

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-${var.environment}-gold"
    Environment = var.environment
    Layer       = "gold"
    Purpose     = "curated-data"
  })
}

# Logs Bucket
resource "aws_s3_bucket" "logs" {
  bucket = "${var.project_name}-${var.environment}-logs-${random_id.bucket_suffix.hex}"

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-${var.environment}-logs"
    Environment = var.environment
    Purpose     = "logs"
  })
}

# Scripts Bucket
resource "aws_s3_bucket" "scripts" {
  bucket = "${var.project_name}-${var.environment}-scripts-${random_id.bucket_suffix.hex}"

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-${var.environment}-scripts"
    Environment = var.environment
    Purpose     = "scripts"
  })
}

# Bucket Versioning - Bronze
resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Disabled"
  }
}

# Bucket Versioning - Silver
resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Disabled"
  }
}

# Bucket Versioning - Gold
resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Disabled"
  }
}

# Lifecycle Configuration - Bronze (Raw data)
resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  count  = var.enable_lifecycle_management ? 1 : 0
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "bronze_lifecycle"
    status = "Enabled"

    transition {
      days          = var.bronze_transition_to_ia_days
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = var.bronze_transition_to_glacier_days
      storage_class = "GLACIER"
    }

    expiration {
      days = var.bronze_expiration_days
    }
  }
}

# Lifecycle Configuration - Silver (Processed data)
resource "aws_s3_bucket_lifecycle_configuration" "silver" {
  count  = var.enable_lifecycle_management ? 1 : 0
  bucket = aws_s3_bucket.silver.id

  rule {
    id     = "silver_lifecycle"
    status = "Enabled"

    transition {
      days          = var.silver_transition_to_ia_days
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = var.silver_transition_to_glacier_days
      storage_class = "GLACIER"
    }
  }
}

# Server-side encryption - Bronze
resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  count  = var.enable_encryption ? 1 : 0
  bucket = aws_s3_bucket.bronze.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# Server-side encryption - Silver
resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  count  = var.enable_encryption ? 1 : 0
  bucket = aws_s3_bucket.silver.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# Server-side encryption - Gold
resource "aws_s3_bucket_server_side_encryption_configuration" "gold" {
  count  = var.enable_encryption ? 1 : 0
  bucket = aws_s3_bucket.gold.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# Public access block - Bronze
resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Public access block - Silver
resource "aws_s3_bucket_public_access_block" "silver" {
  bucket = aws_s3_bucket.silver.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Public access block - Gold
resource "aws_s3_bucket_public_access_block" "gold" {
  bucket = aws_s3_bucket.gold.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
