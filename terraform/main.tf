data "aws_caller_identity" "current" {}

locals {
  name        = "${var.project_name}-${var.environment}"
  account_id  = data.aws_caller_identity.current.account_id
  bucket_name = var.bucket_name != "" ? var.bucket_name : "${local.name}-${local.account_id}"
  deployment_principal = (
    var.deployment_principal_arn != ""
    ? var.deployment_principal_arn
    : "arn:aws:iam::${local.account_id}:root"
  )

  # MWAA escribe sus logs en log groups con nombre fijo:
  # airflow-<nombre-del-ambiente>-<componente>. Se crean aqui para fijar retencion.
  mwaa_env_name       = local.name
  mwaa_env_arn        = "arn:aws:airflow:${var.aws_region}:${local.account_id}:environment/${local.mwaa_env_name}"
  mwaa_log_components = ["DAGProcessing", "Scheduler", "Task", "WebServer", "Worker"]
  mwaa_log_group_arn  = "arn:aws:logs:${var.aws_region}:${local.account_id}:log-group:airflow-${local.mwaa_env_name}-*"

  # Namespace de las metricas que publica src/metrics.py.
  metrics_namespace = "MercadoNube/Pipeline"

  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Purpose     = "mercado-nube-data-platform"
  }
}

# ---------------------------------------------------------------------------
# Data lake: raw -> cleaned -> curated (prefijos) en un bucket por ambiente.
# En local se usa Moto; este bloque es el equivalente AWS.
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

    noncurrent_version_expiration {
      noncurrent_days = 30
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

    noncurrent_version_expiration {
      noncurrent_days = 30
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

    noncurrent_version_expiration {
      noncurrent_days = 30
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
# Rol de EJECUCION (MWAA). Permisos minimos segun la guia oficial de MWAA:
# - lee codigo (dags/, requirements/) y escribe SOLO en raw/, cleaned/, curated/;
# - logs solo en sus log groups airflow-<ambiente>-*;
# - cola SQS interna de Celery y KMS solo via SQS;
# - metricas solo en los namespaces del proyecto y de MWAA.
# Los "*" que quedan son de acciones que AWS no permite acotar por recurso
# (DescribeLogGroups, GetAccountPublicAccessBlock, PutMetricData) y se
# compensan con condiciones.
# ---------------------------------------------------------------------------
resource "aws_iam_role" "mwaa" {
  name = "${local.name}-mwaa-execution"

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
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = local.account_id
          }
        }
      }
    ]
  })
}

locals {
  mwaa_warehouse_statements = var.enable_redshift ? [
    {
      Sid      = "ReadWarehouseSecret"
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
      Resource = [aws_redshiftserverless_namespace.this[0].admin_password_secret_arn]
    }
  ] : []
}

resource "aws_iam_role_policy" "mwaa" {
  name = "${local.name}-mwaa-data-access"
  role = aws_iam_role.mwaa.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Sid      = "PublishMwaaMetrics"
        Effect   = "Allow"
        Action   = ["airflow:PublishMetrics"]
        Resource = [local.mwaa_env_arn]
      },
      {
        Sid      = "DenyListAllBuckets"
        Effect   = "Deny"
        Action   = ["s3:ListAllMyBuckets"]
        Resource = [aws_s3_bucket.data.arn, "${aws_s3_bucket.data.arn}/*"]
      },
      {
        Sid    = "ReadProjectBucket"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:GetBucketLocation",
          "s3:GetBucketPublicAccessBlock",
          "s3:GetBucketVersioning",
          "s3:ListBucket"
        ]
        Resource = [aws_s3_bucket.data.arn, "${aws_s3_bucket.data.arn}/*"]
      },
      {
        Sid    = "WriteDataLayersOnly"
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:DeleteObject"]
        Resource = [
          "${aws_s3_bucket.data.arn}/raw/*",
          "${aws_s3_bucket.data.arn}/cleaned/*",
          "${aws_s3_bucket.data.arn}/curated/*"
        ]
      },
      {
        Sid      = "AccountPublicAccessBlockCheck"
        Effect   = "Allow"
        Action   = ["s3:GetAccountPublicAccessBlock"]
        Resource = ["*"]
      },
      {
        Sid    = "WriteMwaaLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:CreateLogGroup",
          "logs:PutLogEvents",
          "logs:GetLogEvents",
          "logs:GetLogRecord",
          "logs:GetLogGroupFields",
          "logs:GetQueryResults"
        ]
        Resource = [local.mwaa_log_group_arn, "${local.mwaa_log_group_arn}:*"]
      },
      {
        Sid      = "DescribeLogGroups"
        Effect   = "Allow"
        Action   = ["logs:DescribeLogGroups"]
        Resource = ["*"]
      },
      {
        Sid      = "PutPipelineMetrics"
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = ["*"]
        Condition = {
          StringEquals = {
            "cloudwatch:namespace" = [local.metrics_namespace, "AmazonMWAA"]
          }
        }
      },
      {
        Sid    = "CelerySqsQueue"
        Effect = "Allow"
        Action = [
          "sqs:ChangeMessageVisibility",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl",
          "sqs:ReceiveMessage",
          "sqs:SendMessage"
        ]
        Resource = ["arn:aws:sqs:${var.aws_region}:*:airflow-celery-*"]
      },
      {
        Sid         = "KmsOnlyThroughSqs"
        Effect      = "Allow"
        Action      = ["kms:Decrypt", "kms:DescribeKey", "kms:GenerateDataKey*", "kms:Encrypt"]
        NotResource = ["arn:aws:kms:*:${local.account_id}:key/*"]
        Condition = {
          StringLike = {
            "kms:ViaService" = ["sqs.${var.aws_region}.amazonaws.com"]
          }
        }
      }
    ], local.mwaa_warehouse_statements)
  })
}

# ---------------------------------------------------------------------------
# Rol de DESPLIEGUE, separado del de ejecucion. Lo asume Terraform/CI.
# No tiene permisos sobre los objetos del bucket (datos de negocio).
# ---------------------------------------------------------------------------
resource "aws_iam_role" "deployment" {
  name        = "${local.name}-deployment"
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
  name = "${local.name}-deployment-policy"
  role = aws_iam_role.deployment.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ManageProjectBucketConfig"
        Effect = "Allow"
        Action = [
          "s3:CreateBucket",
          "s3:DeleteBucket",
          "s3:ListBucket",
          "s3:GetBucket*",
          "s3:GetAccelerateConfiguration",
          "s3:GetEncryptionConfiguration",
          "s3:GetLifecycleConfiguration",
          "s3:GetReplicationConfiguration",
          "s3:PutBucketPolicy",
          "s3:DeleteBucketPolicy",
          "s3:PutBucketVersioning",
          "s3:PutEncryptionConfiguration",
          "s3:PutLifecycleConfiguration",
          "s3:PutBucketPublicAccessBlock",
          "s3:PutBucketOwnershipControls",
          "s3:PutBucketTagging"
        ]
        Resource = [aws_s3_bucket.data.arn]
      },
      {
        Sid    = "ManageProjectRoles"
        Effect = "Allow"
        Action = [
          "iam:GetRole",
          "iam:CreateRole",
          "iam:DeleteRole",
          "iam:TagRole",
          "iam:UntagRole",
          "iam:ListRoleTags",
          "iam:UpdateAssumeRolePolicy",
          "iam:PutRolePolicy",
          "iam:GetRolePolicy",
          "iam:DeleteRolePolicy",
          "iam:ListRolePolicies",
          "iam:ListAttachedRolePolicies",
          "iam:ListInstanceProfilesForRole"
        ]
        Resource = [
          aws_iam_role.mwaa.arn,
          "arn:aws:iam::${local.account_id}:role/${local.name}-redshift-copy"
        ]
      },
      {
        Sid      = "PassRolesOnlyToTheirService"
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = [aws_iam_role.mwaa.arn, "arn:aws:iam::${local.account_id}:role/${local.name}-redshift-copy"]
        Condition = {
          StringEquals = {
            "iam:PassedToService" = ["airflow.amazonaws.com", "redshift.amazonaws.com", "redshift-serverless.amazonaws.com"]
          }
        }
      },
      {
        Sid    = "ManageLogGroups"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:DeleteLogGroup",
          "logs:PutRetentionPolicy",
          "logs:DeleteRetentionPolicy",
          "logs:PutMetricFilter",
          "logs:DeleteMetricFilter",
          "logs:DescribeMetricFilters",
          "logs:TagResource",
          "logs:UntagResource",
          "logs:ListTagsForResource",
          "logs:TagLogGroup",
          "logs:ListTagsLogGroup"
        ]
        Resource = [local.mwaa_log_group_arn, "${local.mwaa_log_group_arn}:*"]
      },
      {
        Sid      = "DescribeLogGroupsInAccount"
        Effect   = "Allow"
        Action   = ["logs:DescribeLogGroups"]
        Resource = ["arn:aws:logs:${var.aws_region}:${local.account_id}:log-group:*"]
      },
      {
        Sid    = "ManageProjectAlarms"
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricAlarm",
          "cloudwatch:DeleteAlarms",
          "cloudwatch:DescribeAlarms",
          "cloudwatch:TagResource",
          "cloudwatch:UntagResource",
          "cloudwatch:ListTagsForResource"
        ]
        Resource = ["arn:aws:cloudwatch:${var.aws_region}:${local.account_id}:alarm:${local.name}-*"]
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
          "sns:UntagResource",
          "sns:ListTagsForResource"
        ]
        Resource = [aws_sns_topic.alerts.arn]
      },
      {
        Sid    = "ManageMwaaEnvironment"
        Effect = "Allow"
        Action = [
          "airflow:CreateEnvironment",
          "airflow:UpdateEnvironment",
          "airflow:DeleteEnvironment",
          "airflow:GetEnvironment",
          "airflow:TagResource",
          "airflow:UntagResource",
          "airflow:ListTagsForResource"
        ]
        Resource = [local.mwaa_env_arn]
      },
      {
        Sid    = "ManageWarehouse"
        Effect = "Allow"
        Action = [
          "redshift-serverless:CreateNamespace",
          "redshift-serverless:UpdateNamespace",
          "redshift-serverless:DeleteNamespace",
          "redshift-serverless:GetNamespace",
          "redshift-serverless:CreateWorkgroup",
          "redshift-serverless:UpdateWorkgroup",
          "redshift-serverless:DeleteWorkgroup",
          "redshift-serverless:GetWorkgroup",
          "redshift-serverless:TagResource",
          "redshift-serverless:UntagResource",
          "redshift-serverless:ListTagsForResource"
        ]
        Resource = [
          "arn:aws:redshift-serverless:${var.aws_region}:${local.account_id}:namespace/*",
          "arn:aws:redshift-serverless:${var.aws_region}:${local.account_id}:workgroup/*"
        ]
      },
      {
        Sid    = "ManageWarehouseAdminSecret"
        Effect = "Allow"
        Action = [
          "secretsmanager:CreateSecret",
          "secretsmanager:DeleteSecret",
          "secretsmanager:DescribeSecret",
          "secretsmanager:RotateSecret",
          "secretsmanager:TagResource"
        ]
        Resource = ["arn:aws:secretsmanager:${var.aws_region}:${local.account_id}:secret:redshift!*"]
      },
      {
        Sid      = "ManageProjectBudget"
        Effect   = "Allow"
        Action   = ["budgets:ViewBudget", "budgets:ModifyBudget"]
        Resource = ["arn:aws:budgets::${local.account_id}:budget/${local.name}-*"]
      },
      {
        Sid    = "ReadNetworkForMwaaAndRedshift"
        Effect = "Allow"
        Action = [
          "ec2:DescribeVpcs",
          "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeAvailabilityZones"
        ]
        # Las acciones Describe de EC2 no admiten restriccion por recurso.
        Resource = ["*"]
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Logs de MWAA con retencion definida (MWAA reutiliza estos log groups).
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "mwaa" {
  for_each = toset(local.mwaa_log_components)

  name              = "airflow-${local.mwaa_env_name}-${each.key}"
  retention_in_days = var.log_retention_days
}

# ---------------------------------------------------------------------------
# Alertas: fallos, aumento de rechazos, fallo de calidad y retraso de datos.
# ---------------------------------------------------------------------------
resource "aws_sns_topic" "alerts" {
  name = "${local.name}-alerts"
}

resource "aws_sns_topic_policy" "alerts" {
  arn = aws_sns_topic.alerts.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowAccountOwner"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${local.account_id}:root" }
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
            "aws:SourceAccount" = local.account_id
          }
        }
      }
    ]
  })
}

resource "aws_sns_topic_subscription" "alerts_email" {
  count = var.alert_email != "" ? 1 : 0

  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Fallos de tareas: cualquier ERROR en los logs de tareas de Airflow.
resource "aws_cloudwatch_log_metric_filter" "pipeline_errors" {
  name           = "${local.name}-pipeline-errors"
  log_group_name = aws_cloudwatch_log_group.mwaa["Task"].name
  pattern        = "ERROR"

  metric_transformation {
    name          = "PipelineErrors"
    namespace     = local.metrics_namespace
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "pipeline_errors" {
  alarm_name          = "${local.name}-pipeline-errors"
  alarm_description   = "Una o mas tareas registraron ERROR en CloudWatch Logs."
  namespace           = local.metrics_namespace
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

# Aumento de rechazos: la tasa del lote (src/metrics.py) supera el umbral.
resource "aws_cloudwatch_metric_alarm" "rejection_rate" {
  alarm_name          = "${local.name}-rejection-rate-order-events"
  alarm_description   = "La tasa de rechazo de eventos supero el umbral definido."
  namespace           = local.metrics_namespace
  metric_name         = "RejectionRatePct"
  dimensions          = { Source = "order_events" }
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  period              = 3600
  statistic           = "Maximum"
  threshold           = var.max_rejection_rate_pct
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

# Fallo del quality gate final.
resource "aws_cloudwatch_metric_alarm" "quality_gate_failed" {
  alarm_name          = "${local.name}-quality-gate-failed"
  alarm_description   = "El quality gate final de un lote fallo."
  namespace           = local.metrics_namespace
  metric_name         = "QualityGateFailed"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  period              = 3600
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

# Retraso de datos: ningun lote publicado con exito en 24 horas.
# Solo existe con MWAA activo; sin pipeline corriendo estaria siempre en alarma.
resource "aws_cloudwatch_metric_alarm" "data_delay" {
  count = var.enable_mwaa ? 1 : 0

  alarm_name          = "${local.name}-data-delay"
  alarm_description   = "No se publico ningun lote sano en las ultimas 24 horas."
  namespace           = local.metrics_namespace
  metric_name         = "BatchesPublished"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 1
  period              = 86400
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "mwaa_dag_processing_errors" {
  count = var.enable_mwaa ? 1 : 0

  alarm_name          = "${local.name}-mwaa-dag-errors"
  alarm_description   = "MWAA reporto errores de procesamiento de DAGs."
  namespace           = "AmazonMWAA"
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

  alarm_name          = "${local.name}-mwaa-scheduler-heartbeat"
  alarm_description   = "El scheduler de MWAA no reporta heartbeat en 5 minutos."
  namespace           = "AmazonMWAA"
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
# MWAA opcional (enable_mwaa=false por defecto: ~USD 360/mes).
# ---------------------------------------------------------------------------
resource "aws_mwaa_environment" "this" {
  count = var.enable_mwaa ? 1 : 0

  name               = local.mwaa_env_name
  airflow_version    = "2.10.3"
  environment_class  = "mw1.small"
  execution_role_arn = aws_iam_role.mwaa.arn
  source_bucket_arn  = aws_s3_bucket.data.arn
  dag_s3_path        = "dags"
  max_workers        = 2

  # Variables no secretas para el pipeline. Las credenciales del warehouse
  # se leen de Secrets Manager con el rol de ejecucion, nunca de aqui.
  airflow_configuration_options = {
    "core.default_timezone" = "utc"
  }

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

  depends_on = [aws_cloudwatch_log_group.mwaa]
}

# ---------------------------------------------------------------------------
# Warehouse: Redshift Serverless opcional (enable_redshift=false por defecto).
# - Contrasena administrada por Secrets Manager (no hay secretos en Terraform).
# - Rol de COPY que solo puede LEER curated/ del data lake.
# ---------------------------------------------------------------------------
resource "aws_iam_role" "redshift_copy" {
  count = var.enable_redshift ? 1 : 0

  name = "${local.name}-redshift-copy"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"]
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "redshift_copy" {
  count = var.enable_redshift ? 1 : 0

  name = "${local.name}-redshift-read-curated"
  role = aws_iam_role.redshift_copy[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadCuratedObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${aws_s3_bucket.data.arn}/curated/*"]
      },
      {
        Sid      = "ListCuratedPrefix"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [aws_s3_bucket.data.arn]
        Condition = {
          StringLike = {
            "s3:prefix" = ["curated/*"]
          }
        }
      }
    ]
  })
}

resource "aws_redshiftserverless_namespace" "this" {
  count = var.enable_redshift ? 1 : 0

  namespace_name        = local.name
  db_name               = "mercado_nube"
  admin_username        = "mn_admin"
  manage_admin_password = true
  iam_roles             = [aws_iam_role.redshift_copy[0].arn]
  default_iam_role_arn  = aws_iam_role.redshift_copy[0].arn
}

resource "aws_redshiftserverless_workgroup" "this" {
  count = var.enable_redshift ? 1 : 0

  namespace_name      = aws_redshiftserverless_namespace.this[0].namespace_name
  workgroup_name      = local.name
  base_capacity       = var.redshift_base_capacity
  publicly_accessible = false
  subnet_ids          = length(var.redshift_subnet_ids) > 0 ? var.redshift_subnet_ids : null
  security_group_ids  = length(var.redshift_security_group_ids) > 0 ? var.redshift_security_group_ids : null
}

# ---------------------------------------------------------------------------
# Limite de costo: presupuesto mensual con aviso al 80 % real y 100 % proyectado.
# ---------------------------------------------------------------------------
resource "aws_budgets_budget" "monthly" {
  count = var.alert_email != "" ? 1 : 0

  name         = "${local.name}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}
