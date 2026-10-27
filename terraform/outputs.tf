output "data_bucket_name" {
  description = "Bucket para raw/cleaned/curated."
  value       = aws_s3_bucket.data.bucket
}

output "data_bucket_arn" {
  description = "ARN del data lake."
  value       = aws_s3_bucket.data.arn
}

output "mwaa_execution_role_arn" {
  description = "Rol que asume MWAA para ejecutar los DAGs."
  value       = aws_iam_role.mwaa.arn
}

output "deployment_role_arn" {
  description = "Rol separado para Terraform/CI; no ejecuta DAGs ni lee datos."
  value       = aws_iam_role.deployment.arn
}

output "alerts_topic_arn" {
  description = "Topic SNS de alarmas (fallos, rechazos, calidad, retraso de datos)."
  value       = aws_sns_topic.alerts.arn
}

output "mwaa_log_group_names" {
  description = "Log groups de MWAA con retencion definida."
  value       = [for group in aws_cloudwatch_log_group.mwaa : group.name]
}

output "pipeline_metrics_namespace" {
  description = "Namespace de las metricas que publica src/metrics.py."
  value       = local.metrics_namespace
}

output "mwaa_environment_name" {
  description = "Nombre del ambiente MWAA si enable_mwaa=true."
  value       = try(aws_mwaa_environment.this[0].name, null)
}

output "redshift_workgroup_endpoint" {
  description = "Endpoint de Redshift Serverless si enable_redshift=true."
  value       = try(aws_redshiftserverless_workgroup.this[0].endpoint, null)
}

output "redshift_copy_role_arn" {
  description = "Rol IAM que usa COPY para leer curated/ (si enable_redshift=true)."
  value       = try(aws_iam_role.redshift_copy[0].arn, null)
}

output "redshift_admin_secret_arn" {
  description = "Secreto administrado con la clave del admin de Redshift."
  value       = try(aws_redshiftserverless_namespace.this[0].admin_password_secret_arn, null)
}
