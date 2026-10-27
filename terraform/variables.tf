variable "project_name" {
  description = "Prefijo de recursos. Se usa tambien en tags."
  type        = string
  default     = "mercado-nube"
}

variable "environment" {
  description = "Ambiente logico (dev, staging, prod)."
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
  description = "Si true, crea el ambiente MWAA. Requiere VPC, subredes y SGs."
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

variable "deployment_principal_arn" {
  description = "ARN del usuario/rol de CI que puede asumir el rol de despliegue. Vacio = root de la cuenta (solo para laboratorio)."
  type        = string
  default     = ""
}
