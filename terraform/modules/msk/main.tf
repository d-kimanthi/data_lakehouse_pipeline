# terraform/modules/msk/main.tf

# KMS Key for MSK encryption
resource "aws_kms_key" "msk" {
  description = "KMS key for MSK cluster encryption"

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-msk-key-${var.environment}"
    Environment = var.environment
    Service     = "msk"
  })
}

resource "aws_kms_alias" "msk" {
  name          = "alias/${var.project_name}-msk-${var.environment}"
  target_key_id = aws_kms_key.msk.key_id
}

# CloudWatch Log Group for MSK
resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${var.project_name}-${var.environment}"
  retention_in_days = var.log_retention_days

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-msk-logs-${var.environment}"
    Environment = var.environment
    Service     = "msk"
  })
}

# MSK Configuration
resource "aws_msk_configuration" "kafka_config" {
  kafka_versions = [var.kafka_version]
  name           = "${var.project_name}-kafka-config-${var.environment}"

  server_properties = <<PROPERTIES
auto.create.topics.enable=true
default.replication.factor=${var.number_of_broker_nodes}
min.insync.replicas=${max(1, var.number_of_broker_nodes - 1)}
num.partitions=3
log.retention.hours=${var.log_retention_hours}
log.retention.bytes=${var.log_retention_bytes}
log.segment.bytes=1073741824
log.cleanup.policy=delete
compression.type=gzip
PROPERTIES

  description = "Kafka configuration for ${var.project_name} ${var.environment}"
}

# Security Group for MSK
resource "aws_security_group" "msk" {
  name_prefix = "${var.project_name}-msk-${var.environment}-"
  vpc_id      = var.vpc_id
  description = "Security group for MSK cluster"

  # Kafka plaintext
  ingress {
    description = "Kafka plaintext"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr_block]
  }

  # Kafka TLS
  ingress {
    description = "Kafka TLS"
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr_block]
  }

  # Kafka SASL_SSL
  ingress {
    description = "Kafka SASL_SSL"
    from_port   = 9096
    to_port     = 9096
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr_block]
  }

  # Zookeeper
  ingress {
    description = "Zookeeper"
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr_block]
  }

  # JMX
  ingress {
    description = "JMX"
    from_port   = 11001
    to_port     = 11002
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr_block]
  }

  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-msk-sg-${var.environment}"
    Environment = var.environment
    Service     = "msk"
  })
}

# MSK Cluster
resource "aws_msk_cluster" "main" {
  cluster_name           = "${var.project_name}-kafka-${var.environment}"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.number_of_broker_nodes

  broker_node_group_info {
    instance_type  = var.instance_type
    client_subnets = var.private_subnet_ids

    storage_info {
      ebs_storage_info {
        volume_size = var.ebs_volume_size
      }
    }

    security_groups = [aws_security_group.msk.id]
  }

  configuration_info {
    arn      = aws_msk_configuration.kafka_config.arn
    revision = aws_msk_configuration.kafka_config.latest_revision
  }

  dynamic "encryption_info" {
    for_each = var.enable_encryption ? [1] : []
    content {
      encryption_at_rest_kms_key_arn = aws_kms_key.msk.arn
      encryption_in_transit {
        client_broker = var.client_broker_encryption
        in_cluster    = var.in_cluster_encryption
      }
    }
  }

  dynamic "client_authentication" {
    for_each = var.enable_client_authentication ? [1] : []
    content {
      tls {
        certificate_authority_arns = var.certificate_authority_arns
      }
    }
  }

  dynamic "logging_info" {
    for_each = var.enable_logging ? [1] : []
    content {
      broker_logs {
        cloudwatch_logs {
          enabled   = var.enable_cloudwatch_logs
          log_group = aws_cloudwatch_log_group.msk.name
        }
        s3 {
          enabled = var.enable_s3_logs
          bucket  = var.logs_bucket_id
          prefix  = "msk-logs/"
        }
      }
    }
  }

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-kafka-${var.environment}"
    Environment = var.environment
    Service     = "msk"
  })
}
