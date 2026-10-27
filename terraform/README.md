# Terraform — infraestructura AWS (referencia validada, no aplicada)

Este directorio describe la infraestructura para ejecutar el mismo pipeline en AWS. **No se ejecutó `terraform apply` en una cuenta real**: la demo es 100% local y sin costo. La configuración pasa `fmt` y `validate`, y se probó un `apply` contra el emulador Moto (S3, IAM, CloudWatch, SNS) para comprobar que las políticas y recursos se crean sin errores.

## Qué crea

| Recurso | Detalle | Por defecto |
|---|---|---|
| Bucket S3 por ambiente | `raw/`, `cleaned/`, `curated/`; versionado; SSE-S3; bloqueo público; solo TLS; lifecycle por capa (IA y expiración) | Sí |
| Rol de **ejecución** (MWAA) | Lee el bucket, escribe solo en `raw/ cleaned/ curated/`, logs `airflow-<amb>-*`, SQS/KMS de Celery, métricas con namespace restringido | Sí |
| Rol de **despliegue** (Terraform/CI) | Administra solo los recursos del proyecto; sin acceso a objetos de datos | Sí |
| Log groups de MWAA | `airflow-<amb>-{DAGProcessing,Scheduler,Task,WebServer,Worker}` con retención | Sí |
| Alarmas + SNS | Errores en logs, aumento de rechazos, fallo de calidad, retraso de datos (24 h), salud de MWAA | Sí (retraso y MWAA solo con `enable_mwaa`) |
| MWAA `mw1.small` | Airflow administrado | `enable_mwaa=false` |
| Redshift Serverless | Namespace + workgroup privados, clave administrada por Secrets Manager, rol de `COPY` que solo lee `curated/` | `enable_redshift=false` |
| AWS Budgets | Aviso al 80 % real y 100 % proyectado del límite mensual | Solo si `alert_email` tiene valor |

## Métricas y alarmas

`src/metrics.py` publica en el namespace `MercadoNube/Pipeline` cuando `CLOUDWATCH_METRICS_ENABLED=true` (en local solo escribe `METRIC {...}` en el log de la tarea):

| Métrica | Origen | Alarma |
|---|---|---|
| `RejectionRatePct` (dim. `Source`) | validación previa a publicar | `> max_rejection_rate_pct` (10 %) → aumento de rechazos |
| `DuplicateRatePct` | validación previa | informativa |
| `QualityGateFailed` | quality gate final | `>= 1` → fallo de calidad |
| `BatchesPublished` | quality gate final | `< 1` en 24 h → retraso de datos |
| `PipelineErrors` | metric filter `ERROR` en logs de tareas | `>= 1` en 5 min → fallo |

## Validación realizada

    cd terraform
    terraform init -backend=false
    terraform fmt -check
    terraform validate

## Variables principales

| Variable | Default | Uso |
|---|---|---|
| `project_name` | `mercado-nube` | prefijo y tags |
| `environment` | `dev` | separa bucket, roles y alarmas por ambiente |
| `aws_region` | `us-east-1` | región |
| `bucket_name` | `""` | vacío = nombre único con account id |
| `log_retention_days` | `30` | retención de logs |
| `enable_mwaa` | `false` | evita ~USD 360/mes por defecto |
| `mwaa_subnet_ids` / `mwaa_security_group_ids` | `[]` | obligatorios si `enable_mwaa=true` |
| `enable_redshift` | `false` | evita costo de RPU por defecto |
| `redshift_base_capacity` | `8` | RPU base (mínimo) |
| `redshift_subnet_ids` / `redshift_security_group_ids` | `[]` | red privada de Redshift |
| `deployment_principal_arn` | `""` | rol de CI que asume el rol de despliegue; vacío = root (solo laboratorio) |
| `alert_email` | `""` | suscripción SNS y presupuesto |
| `monthly_budget_usd` | `20` | límite mensual del laboratorio |
| `max_rejection_rate_pct` | `10` | umbral de la alarma de rechazos |

## Roles: despliegue vs. ejecución

| Rol | Quién lo asume | Puede | No puede |
|---|---|---|---|
| Despliegue | Terraform/CI | Crear/actualizar bucket (configuración), roles del proyecto, logs, alarmas, SNS, MWAA, Redshift, presupuesto | Leer o escribir objetos de datos |
| Ejecución | MWAA | Leer código y datos, escribir en las capas de datos, logs, métricas, leer el secreto del warehouse | Crear infraestructura, sobrescribir `dags/` |
| Redshift COPY | Redshift Serverless | Leer `curated/` | Escribir en S3 |

Los únicos `Resource = "*"` corresponden a acciones que AWS no permite acotar por recurso (`logs:DescribeLogGroups`, `s3:GetAccountPublicAccessBlock`, `cloudwatch:PutMetricData` con condición de namespace, `ec2:Describe*`).

## Credenciales

- No hay access keys en el código, en imágenes ni en Terraform.
- MWAA usa su rol de ejecución (credenciales temporales de STS).
- La clave del admin de Redshift la genera y guarda Secrets Manager (`manage_admin_password = true`); el rol de ejecución solo puede leer ese secreto.
- En local, los secretos viven en `.env` (generado por `run_demo.ps1`, fuera de Git).

## Ruta de despliegue (cuenta de laboratorio autorizada)

1. `terraform plan -var alert_email=<correo>` con el rol de despliegue.
2. `terraform apply` con `enable_mwaa=false` y `enable_redshift=false` (costo casi cero) y confirmar el correo de SNS.
3. Crear VPC/subredes privadas/SGs; `enable_redshift=true` y ejecutar `sql/redshift/ddl_redshift.sql`.
4. `enable_mwaa=true`; subir `dags/`, `src/`, `sql/` a `s3://<bucket>/dags/` y `requirements.txt`.
5. Variables de MWAA: `CLOUDWATCH_METRICS_ENABLED=true`, endpoint S3 real, `DAILY_SCHEDULE=@daily`.
6. Cargar el warehouse con `sql/redshift/load_from_s3.sql` (COPY desde `curated/` + MERGE).

## Costos aproximados

Supuestos en `us-east-1`; precios de referencia, verifica la calculadora de AWS.

| Recurso | Supuesto | Aproximado |
|---|---|---:|
| S3 Standard | 10 GB + lifecycle | ~USD 0.23/mes + requests |
| CloudWatch Logs + 4 alarmas | 1 GB ingerido, 30 días | ~USD 1/mes |
| MWAA `mw1.small` | 730 h | ~USD 360/mes |
| Redshift Serverless | 8 RPU, 1 h de uso al día | ~USD 2.88/h de uso ≈ USD 90/mes |
| VPC/NAT | No incluido | puede superar a S3/CloudWatch |

## Limpieza

Local (borra volúmenes del proyecto):

    docker compose down -v

AWS, con el mismo state:

    cd terraform
    # 1. Vaciar el bucket versionado (versiones y delete markers):
    aws s3api delete-objects --bucket <bucket> --delete "$(aws s3api list-object-versions --bucket <bucket> --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' --output json)"
    aws s3api delete-objects --bucket <bucket> --delete "$(aws s3api list-object-versions --bucket <bucket> --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' --output json)"
    # 2. Destruir con las mismas variables con que se aplicó:
    terraform destroy -var enable_mwaa=false -var enable_redshift=false

`force_destroy=false` obliga a vaciar el bucket antes de destruirlo; es una protección deliberada.
