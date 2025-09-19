# terraform/modules/networking/main.tf

# VPC
resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-vpc-${var.environment}"
    Environment = var.environment
  })
}

# Internet Gateway
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-igw-${var.environment}"
    Environment = var.environment
  })
}

# Public Subnets
resource "aws_subnet" "public" {
  count                   = var.availability_zones_count
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.${count.index + 101}.0/24"
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-public-subnet-${count.index + 1}-${var.environment}"
    Environment = var.environment
    Type        = "public"
  })
}

# Private Subnets
resource "aws_subnet" "private" {
  count             = var.availability_zones_count
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index + 1}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-private-subnet-${count.index + 1}-${var.environment}"
    Environment = var.environment
    Type        = "private"
  })
}

# Elastic IPs for NAT Gateways
resource "aws_eip" "nat" {
  count  = var.create_nat_gateways ? var.availability_zones_count : 0
  domain = "vpc"

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-nat-eip-${count.index + 1}-${var.environment}"
    Environment = var.environment
  })
}

# NAT Gateways
resource "aws_nat_gateway" "main" {
  count         = var.create_nat_gateways ? var.availability_zones_count : 0
  allocation_id = aws_eip.nat[count.index].id
  subnet_id     = aws_subnet.public[count.index].id

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-nat-${count.index + 1}-${var.environment}"
    Environment = var.environment
  })

  depends_on = [aws_internet_gateway.main]
}

# Public Route Table
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-public-rt-${var.environment}"
    Environment = var.environment
    Type        = "public"
  })
}

# Private Route Tables
resource "aws_route_table" "private" {
  count  = var.availability_zones_count
  vpc_id = aws_vpc.main.id

  dynamic "route" {
    for_each = var.create_nat_gateways ? [1] : []
    content {
      cidr_block     = "0.0.0.0/0"
      nat_gateway_id = aws_nat_gateway.main[count.index].id
    }
  }

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-private-rt-${count.index + 1}-${var.environment}"
    Environment = var.environment
    Type        = "private"
  })
}

# Route Table Associations - Public
resource "aws_route_table_association" "public" {
  count          = var.availability_zones_count
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# Route Table Associations - Private
resource "aws_route_table_association" "private" {
  count          = var.availability_zones_count
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[count.index].id
}

# Data sources
data "aws_availability_zones" "available" {
  state = "available"
}
