# terraform/modules/emr/outputs.tf

output "cluster_id" {
  description = "ID of the EMR cluster"
  value       = aws_emr_cluster.main.id
}

output "cluster_name" {
  description = "Name of the EMR cluster"
  value       = aws_emr_cluster.main.name
}

output "cluster_arn" {
  description = "ARN of the EMR cluster"
  value       = aws_emr_cluster.main.arn
}

output "master_public_dns" {
  description = "Master public DNS"
  value       = aws_emr_cluster.main.master_public_dns
}

output "service_role_arn" {
  description = "ARN of the EMR service role"
  value       = aws_iam_role.emr_service.arn
}

output "ec2_role_arn" {
  description = "ARN of the EMR EC2 role"
  value       = aws_iam_role.emr_ec2.arn
}

output "master_security_group_id" {
  description = "ID of the EMR master security group"
  value       = aws_security_group.emr_master.id
}

output "worker_security_group_id" {
  description = "ID of the EMR worker security group"
  value       = aws_security_group.emr_worker.id
}
