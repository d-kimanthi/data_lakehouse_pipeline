# terraform/modules/emr/main.tf

# IAM Role for EMR Service
resource "aws_iam_role" "emr_service" {
  name = "${var.project_name}-emr-service-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "elasticmapreduce.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-emr-service-role-${var.environment}"
    Environment = var.environment
    Service     = "emr"
  })
}

resource "aws_iam_role_policy_attachment" "emr_service" {
  role       = aws_iam_role.emr_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

# IAM Role for EMR EC2 Instances
resource "aws_iam_role" "emr_ec2" {
  name = "${var.project_name}-emr-ec2-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-emr-ec2-role-${var.environment}"
    Environment = var.environment
    Service     = "emr"
  })
}

resource "aws_iam_role_policy_attachment" "emr_ec2" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

# S3 Access Policy for EMR
resource "aws_iam_role_policy" "emr_s3_access" {
  name = "${var.project_name}-emr-s3-access-${var.environment}"
  role = aws_iam_role.emr_ec2.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "${var.bronze_bucket_arn}",
          "${var.bronze_bucket_arn}/*",
          "${var.silver_bucket_arn}",
          "${var.silver_bucket_arn}/*",
          "${var.gold_bucket_arn}",
          "${var.gold_bucket_arn}/*",
          "${var.logs_bucket_arn}",
          "${var.logs_bucket_arn}/*",
          "${var.scripts_bucket_arn}",
          "${var.scripts_bucket_arn}/*"
        ]
      }
    ]
  })
}

# EMR EC2 Instance Profile
resource "aws_iam_instance_profile" "emr_profile" {
  name = "${var.project_name}-emr-profile-${var.environment}"
  role = aws_iam_role.emr_ec2.name

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-emr-profile-${var.environment}"
    Environment = var.environment
    Service     = "emr"
  })
}

# Security Group for EMR Master
resource "aws_security_group" "emr_master" {
  name_prefix = "${var.project_name}-emr-master-${var.environment}-"
  vpc_id      = var.vpc_id
  description = "Security group for EMR master node"

  # SSH access
  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr_block]
  }

  # Spark UI
  ingress {
    description = "Spark UI"
    from_port   = 4040
    to_port     = 4044
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr_block]
  }

  # Livy
  ingress {
    description = "Livy"
    from_port   = 8998
    to_port     = 8998
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
    Name        = "${var.project_name}-emr-master-sg-${var.environment}"
    Environment = var.environment
    Service     = "emr"
  })
}

# Security Group for EMR Workers
resource "aws_security_group" "emr_worker" {
  name_prefix = "${var.project_name}-emr-worker-${var.environment}-"
  vpc_id      = var.vpc_id
  description = "Security group for EMR worker nodes"

  # Communication with master
  ingress {
    description     = "Communication with master"
    from_port       = 0
    to_port         = 65535
    protocol        = "tcp"
    security_groups = [aws_security_group.emr_master.id]
  }

  # Communication between workers
  ingress {
    description = "Communication between workers"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    self        = true
  }

  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-emr-worker-sg-${var.environment}"
    Environment = var.environment
    Service     = "emr"
  })
}

# EMR Cluster
resource "aws_emr_cluster" "main" {
  name          = "${var.project_name}-spark-${var.environment}"
  release_label = var.release_label
  applications  = var.applications
  service_role  = aws_iam_role.emr_service.arn

  termination_protection            = var.termination_protection
  keep_job_flow_alive_when_no_steps = var.keep_alive

  master_instance_group {
    instance_type = var.master_instance_type
  }

  core_instance_group {
    instance_type  = var.core_instance_type
    instance_count = var.core_instance_count

    ebs_config {
      size                 = var.ebs_root_volume_size
      type                 = "gp3"
      volumes_per_instance = 1
    }
  }

  ec2_attributes {
    subnet_id                         = var.private_subnet_ids[0]
    emr_managed_master_security_group = aws_security_group.emr_master.id
    emr_managed_slave_security_group  = aws_security_group.emr_worker.id
    instance_profile                  = aws_iam_instance_profile.emr_profile.arn
    key_name                          = var.key_name
  }

  configurations_json = jsonencode([
    {
      "Classification" : "spark-defaults"
      "Properties" : merge(var.spark_configuration, {
        "spark.sql.catalog.iceberg.warehouse" : "s3://${var.gold_bucket_id}/iceberg-warehouse/"
      })
    }
  ])

  log_uri = var.logs_bucket_id != "" ? "s3://${var.logs_bucket_id}/emr-logs/" : null

  dynamic "bootstrap_action" {
    for_each = var.bootstrap_actions
    content {
      path = bootstrap_action.value.path
      name = bootstrap_action.value.name
      args = bootstrap_action.value.args
    }
  }

  dynamic "auto_termination_policy" {
    for_each = var.auto_termination_timeout_minutes > 0 ? [1] : []
    content {
      idle_timeout = var.auto_termination_timeout_minutes * 60
    }
  }

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-spark-${var.environment}"
    Environment = var.environment
    Service     = "emr"
  })
}
