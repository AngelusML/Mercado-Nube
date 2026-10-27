data "aws_caller_identity" "current" {}

locals {
  bucket_name = var.bucket_name != "" ? var.bucket_name : "${var.project_name}-${var.environment}-${data.aws_caller_identity.current.account_id}"
  deployment_principal = (
    var.deployment_principal_arn != ""
    ? var.deployment_principal_arn
    : "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
  )
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Purpose     = "mercado-nube-data-platform"
  }
}

# ---------------------------------------------------------------------------
# Data lake: raw -> cleaned -> curated.
# En local se usa Moto; este bloque es el equivalente AWS listo para revisar.
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "data" {
  bucket        = local.bucket_name
  force_destroy = false
}

resource "aws_s3_bucket_ownership_controls" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "raw-retention"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    expiration {
      days = 90
    }
  }

  rule {
    id     = "cleaned-retention"
    status = "Enabled"

    filter {
      prefix = "cleaned/"
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }

    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }

    expiration {
      days = 365
    }
  }

  rule {
    id     = "curated-retention"
    status = "Enabled"

    filter {
      prefix = "curated/"
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    expiration {
      days = 730
    }
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  bucket = aws_s3_bucket.data.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*"
        ]
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Rol de ejecucion MWAA: solo S3 del data lake, logs y metricas MWAA.
# ---------------------------------------------------------------------------
resource "aws_iam_role" "mwaa" {
  name = "${var.project_name}-${var.environment}-mwaa-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = [
            "airflow.amazonaws.com",
            "airflow-env.amazonaws.com"
          ]
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "mwaa" {
  name = "${var.project_name}-${var.environment}-mwaa-data-access"
  role = aws_iam_role.mwaa.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListDataBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:GetBucketPublicAccessBlock"
        ]
        Resource = [aws_s3_bucket.data.arn]
      },
      {
        Sid    = "ReadWriteDataObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = ["${aws_s3_bucket.data.arn}/*"]
      },
      {
        Sid    = "WriteMwaaLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:CreateLogGroup",
          "logs:DescribeLogStreams"
        ]
        Resource = [
          aws_cloudwatch_log_group.mwaa.arn,
          "${aws_cloudwatch_log_group.mwaa.arn}:*"
        ]
      },
      {
        Sid      = "PublishMwaaMetrics"
        Effect   = "Allow"
        Action   = ["airflow:PublishMetrics"]
        Resource = ["*"]
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Rol de despliegue separado del rol de ejecucion. Lo asume el pipeline de CI
# (o un operador autorizado) para crear/actualizar los recursos del proyecto.
# No se usa para ejecutar DAGs y no tiene permisos de datos de negocio.
# ---------------------------------------------------------------------------
resource "aws_iam_role" "deployment" {
  name        = "${var.project_name}-${var.environment}-deployment"
  description = "Rol para Terraform/CI, separado del rol de ejecucion de MWAA."

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { AWS = local.deployment_principal }
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "deployment" {
  name = "${var.project_name}-${var.environment}-deployment-policy"
  role = aws_iam_role.deployment.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ManageProjectBucket"
        Effect = "Allow"
        Action = [
          "s3:CreateBucket",
          "s3:DeleteBucket",
          "s3:GetBucketLocation",
          "s3:GetBucketPolicy",
          "s3:PutBucketPolicy",
          "s3:DeleteBucketPolicy",
          "s3:GetBucketVersioning",
          "s3:PutBucketVersioning",
          "s3:GetEncryptionConfiguration",
          "s3:PutEncryptionConfiguration",
          "s3:GetLifecycleConfiguration",
          "s3:PutLifecycleConfiguration",
          "s3:GetBucketPublicAccessBlock",
          "s3:PutBucketPublicAccessBlock",
          "s3:GetBucketOwnershipControls",
          "s3:PutBucketOwnershipControls",
          "s3:ListBucket",
          "s3:GetBucketAcl"
        ]
        Resource = [aws_s3_bucket.data.arn]
      },
      {
        Sid    = "ManageExecutionRole"
        Effect = "Allow"
        Action = [
          "iam:GetRole",
          "iam:CreateRole",
          "iam:DeleteRole",
          "iam:UpdateAssumeRolePolicy",
          "iam:PutRolePolicy",
          "iam:GetRolePolicy",
          "iam:DeleteRolePolicy",
          "iam:ListRolePolicies",
          "iam:ListAttachedRolePolicies",
          "iam:AttachRolePolicy",
          "iam:DetachRolePolicy",
          "iam:PassRole"
        ]
        Resource = [aws_iam_role.mwaa.arn]
      },
      {
        Sid    = "ManageLogsAndAlarms"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:DeleteLogGroup",
          "logs:DescribeLogGroups",
          "logs:PutRetentionPolicy",
          "logs:DeleteRetentionPolicy",
          "logs:CreateLogStream",
          "logs:DeleteLogStream",
          "logs:DescribeLogStreams",
          "logs:PutLogEvents",
          "logs:FilterLogEvents",
          "cloudwatch:PutMetricAlarm",
          "cloudwatch:DeleteAlarms",
          "cloudwatch:DescribeAlarms",
          "cloudwatch:PutMetricFilter",
          "cloudwatch:DeleteMetricFilter",
          "cloudwatch:DescribeMetricFilters"
        ]
        Resource = [
          aws_cloudwatch_log_group.mwaa.arn,
          "${aws_cloudwatch_log_group.mwaa.arn}:*",
          "arn:aws:cloudwatch:${var.aws_region}:${data.aws_caller_identity.current.account_id}:alarm:${var.project_name}-${var.environment}-*"
        ]
      },
      {
        Sid    = "ManageMwaaEnvironment"
        Effect = "Allow"
        Action = [
          "airflow:CreateEnvironment",
          "airflow:UpdateEnvironment",
          "airflow:DeleteEnvironment",
          "airflow:GetEnvironment",
          "airflow:ListEnvironments",
          "airflow:TagResource",
          "airflow:UntagResource"
        ]
        Resource = [
          "arn:aws:airflow:${var.aws_region}:${data.aws_caller_identity.current.account_id}:environment/${var.project_name}-${var.environment}"
        ]
      },
      {
        Sid    = "ManageAlertsTopic"
        Effect = "Allow"
        Action = [
          "sns:CreateTopic",
          "sns:DeleteTopic",
          "sns:GetTopicAttributes",
          "sns:SetTopicAttributes",
          "sns:Subscribe",
          "sns:Unsubscribe",
          "sns:ListSubscriptionsByTopic",
          "sns:TagResource",
          "sns:UntagResource"
        ]
        Resource = [aws_sns_topic.alerts.arn]
      }
    ]
  })
}

resource "aws_cloudwatch_log_group" "mwaa" {
  name              = "/aws/mwaa/${var.project_name}-${var.environment}"
  retention_in_days = var.log_retention_days
}

# ---------------------------------------------------------------------------
# Alertas: errores de tarea en logs y salud del scheduler/DAG processing.
# ---------------------------------------------------------------------------
resource "aws_sns_topic" "alerts" {
  name = "${var.project_name}-${var.environment}-alerts"
}

resource "aws_sns_topic_policy" "alerts" {
  arn = aws_sns_topic.alerts.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowAccountOwner"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" }
        Action    = "sns:*"
        Resource  = aws_sns_topic.alerts.arn
      },
      {
        Sid       = "AllowCloudWatchAlarms"
        Effect    = "Allow"
        Principal = { Service = "cloudwatch.amazonaws.com" }
        Action    = "sns:Publish"
        Resource  = aws_sns_topic.alerts.arn
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
        }
      }
    ]
  })
}

resource "aws_cloudwatch_log_metric_filter" "pipeline_errors" {
  name           = "${var.project_name}-${var.environment}-pipeline-errors"
  log_group_name = aws_cloudwatch_log_group.mwaa.name
  pattern        = "ERROR"

  metric_transformation {
    name          = "PipelineErrors"
    namespace     = "${var.project_name}/pipeline"
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "pipeline_errors" {
  alarm_name          = "${var.project_name}-${var.environment}-pipeline-errors"
  alarm_description   = "Una o mas tareas/DAGs registraron ERROR en CloudWatch Logs."
  namespace           = aws_cloudwatch_log_metric_filter.pipeline_errors.metric_transformation[0].namespace
  metric_name         = aws_cloudwatch_log_metric_filter.pipeline_errors.metric_transformation[0].name
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "mwaa_dag_processing_errors" {
  count = var.enable_mwaa ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-mwaa-dag-errors"
  alarm_description   = "MWAA reporto errores de procesamiento de DAGs."
  namespace           = "MWAA"
  metric_name         = "DAGProcessingErrors"
  dimensions          = { Environment = aws_mwaa_environment.this[0].name }
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "mwaa_scheduler_heartbeat" {
  count = var.enable_mwaa ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-mwaa-scheduler-heartbeat"
  alarm_description   = "El scheduler de MWAA no reporta heartbeat en 5 minutos."
  namespace           = "MWAA"
  metric_name         = "SchedulerHeartbeat"
  dimensions          = { Environment = aws_mwaa_environment.this[0].name }
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 1
  period              = 300
  statistic           = "Minimum"
  threshold           = 1
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

# ---------------------------------------------------------------------------
# MWAA opcional. Se deja deshabilitado por defecto para que el MVP no cree
# recursos caros. Al habilitarlo, el DAG se sube a s3://bucket/dags/.
# ---------------------------------------------------------------------------
resource "aws_mwaa_environment" "this" {
  count = var.enable_mwaa ? 1 : 0

  name               = "${var.project_name}-${var.environment}"
  airflow_version    = "2.10.3"
  environment_class  = "mw1.small"
  execution_role_arn = aws_iam_role.mwaa.arn
  source_bucket_arn  = aws_s3_bucket.data.arn
  dag_s3_path        = "dags"
  max_workers        = 2

  network_configuration {
    security_group_ids = var.mwaa_security_group_ids
    subnet_ids         = var.mwaa_subnet_ids
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "INFO"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }
}
