output "data_bucket_name" {
  description = "Bucket para raw/cleaned/curated."
  value       = aws_s3_bucket.data.bucket
}

output "data_bucket_arn" {
  description = "ARN del data lake."
  value       = aws_s3_bucket.data.arn
}

output "mwaa_execution_role_arn" {
  description = "Rol que asumiria MWAA (o el runner de Airflow)."
  value       = aws_iam_role.mwaa.arn
}

output "deployment_role_arn" {
  description = "Rol separado para Terraform/CI; no ejecuta DAGs."
  value       = aws_iam_role.deployment.arn
}

output "alerts_topic_arn" {
  description = "Topic SNS para alarmas de errores/frescura; suscribir el canal operativo."
  value       = aws_sns_topic.alerts.arn
}

output "mwaa_log_group_name" {
  description = "Log group de referencia."
  value       = aws_cloudwatch_log_group.mwaa.name
}

output "mwaa_environment_name" {
  description = "Nombre del ambiente MWAA si enable_mwaa=true."
  value       = try(aws_mwaa_environment.this[0].name, null)
}
