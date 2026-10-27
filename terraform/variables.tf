variable "project_name" {
  description = "Prefijo de recursos. Se usa tambien en tags."
  type        = string
  default     = "mercado-nube"
}

variable "environment" {
  description = "Ambiente logico (dev, staging, prod). Separa bucket, roles y alarmas."
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "Region AWS de referencia."
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "Nombre exacto del bucket. Vacio = nombre unico con account id."
  type        = string
  default     = ""
}

variable "log_retention_days" {
  description = "Retencion de logs de MWAA."
  type        = number
  default     = 30
}

variable "enable_mwaa" {
  description = "Si true, crea el ambiente MWAA (~USD 360/mes). Requiere VPC, subredes y SGs."
  type        = bool
  default     = false
}

variable "mwaa_subnet_ids" {
  description = "Dos subredes privadas para MWAA (solo si enable_mwaa=true)."
  type        = list(string)
  default     = []
}

variable "mwaa_security_group_ids" {
  description = "Security groups de MWAA (solo si enable_mwaa=true)."
  type        = list(string)
  default     = []
}

variable "enable_redshift" {
  description = "Si true, crea Redshift Serverless (cobra por RPU-hora en uso)."
  type        = bool
  default     = false
}

variable "redshift_base_capacity" {
  description = "RPU base de Redshift Serverless (8 es el minimo)."
  type        = number
  default     = 8
}

variable "redshift_subnet_ids" {
  description = "Subredes privadas para Redshift Serverless. Vacio = VPC por defecto."
  type        = list(string)
  default     = []
}

variable "redshift_security_group_ids" {
  description = "Security groups para Redshift Serverless."
  type        = list(string)
  default     = []
}

variable "deployment_principal_arn" {
  description = "ARN del usuario/rol de CI que puede asumir el rol de despliegue. Vacio = root de la cuenta (solo para laboratorio)."
  type        = string
  default     = ""
}

variable "alert_email" {
  description = "Correo que recibe alarmas (SNS) y avisos de presupuesto. Vacio = sin suscripcion ni presupuesto."
  type        = string
  default     = ""
}

variable "monthly_budget_usd" {
  description = "Limite mensual de costo para el laboratorio (AWS Budgets)."
  type        = number
  default     = 20
}

variable "max_rejection_rate_pct" {
  description = "Umbral (%) de la alarma de aumento de rechazos de eventos."
  type        = number
  default     = 10
}
