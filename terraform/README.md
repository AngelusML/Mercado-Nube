# Terraform — referencia AWS (no aplicada)

Este directorio contiene una referencia **AWS-ready** de la infraestructura mínima para ejecutar el mismo pipeline en AWS. No se ejecutó `terraform apply` ni se creó ningún recurso: la demo es 100% local y sin costo AWS.

## Qué crea

- Bucket S3 para las capas `raw/`, `cleaned/` y `curated/`:
  - versionado;
  - cifrado SSE-S3 (`AES256`);
  - bloqueo total de acceso público;
  - política que rechaza HTTP sin TLS;
  - lifecycle: raw 90 días, cleaned 365 días, curated 730 días; transiciones a `STANDARD_IA`.
- Rol IAM de ejecución para MWAA:
  - `s3:ListBucket`/lectura/escritura/borrado **solo** en el bucket del proyecto;
  - escritura de logs en el log group del proyecto;
  - `airflow:PublishMetrics`.
- Rol IAM de **despliegue** separado (Terraform/CI), con permisos acotados a los recursos del proyecto; no puede ejecutar DAGs ni leer/escribir objetos del bucket de datos.
- CloudWatch Log Group con retención configurable, metric filter de `ERROR`, alarmas y topic SNS:
  - `PipelineErrors` ≥ 1 en 5 min;
  - `DAGProcessingErrors` de MWAA;
  - `SchedulerHeartbeat` ausente.
- MWAA **opcional** (`enable_mwaa=false` por defecto). Al habilitarlo se requieren VPC, dos subredes privadas y security groups.

## Qué no crea (a propósito)

- **Redshift**: en local PostgreSQL hace de warehouse. La migración requiere decidir Redshift provisionado vs. Serverless, red, usuarios y presupuesto. El modelo y las queries son compatibles; no se finge una conexión local a Redshift.
- **VPC/subredes/SGs**: se reciben como variables para no inventar una topología de red.
- **Secretos**: en AWS deben ir a Secrets Manager/SSM; este repositorio solo usa `.env` local, ignorado por git.
- **Backend remoto**: el estado es local. En un equipo real, configurar backend S3 + DynamoDB (o el mecanismo aprobado).

## Validación local realizada

```powershell
cd terraform
terraform init -backend=false
terraform fmt -check
terraform validate
```

Resultado:

```text
fmt_exit=0
Success! The configuration is valid.
validate_exit=0
```

Provider: `hashicorp/aws v5.100.0`. Ver `docs/evidence/terraform_validate.txt`.

> `init` descarga el provider; no requiere credenciales AWS. `validate` tampoco. **No** se ejecutó `plan` ni `apply`.

## Variables principales

| Variable | Default | Uso |
|---|---|---|
| `project_name` | `mercado-nube` | prefijo y tags |
| `environment` | `dev` | separación lógica |
| `aws_region` | `us-east-1` | región |
| `bucket_name` | `""` | vacío = nombre único con account id |
| `log_retention_days` | `30` | retención CloudWatch |
| `enable_mwaa` | `false` | evita crear MWAA por defecto |
| `mwaa_subnet_ids` | `[]` | obligatorio si `enable_mwaa=true` |
| `mwaa_security_group_ids` | `[]` | obligatorio si `enable_mwaa=true` |
| `deployment_principal_arn` | `""` | usuario/rol CI que asume el rol de despliegue; vacío = root de la cuenta (solo laboratorio) |

## Roles: despliegue vs. ejecución

| Rol | ARN output | Quién lo asume | Permisos |
|---|---|---|---|
| Despliegue | `deployment_role_arn` | Terraform/CI del equipo | Crear/actualizar S3, IAM del proyecto, logs, alarmas, SNS y MWAA |
| Ejecución | `mwaa_execution_role_arn` | MWAA / runner de Airflow | Leer/escribir el bucket, escribir logs y publicar métricas MWAA |

El rol de despliegue **no** es el execution role. Para usarlo, configura el provider o `AWS_PROFILE` con un principal autorizado y define `deployment_principal_arn` al rol de CI; nunca uses el root de la cuenta en producción. El execution role no puede crear infraestructura.

## Ruta de despliegue real (con aprobación)

1. Revisar `terraform plan` con credenciales de una cuenta de pruebas.
2. Aprobar presupuesto (MWAA tiene costo por hora; S3/CloudWatch son menores).
3. Aplicar el bucket/IAM/log group con `enable_mwaa=false`.
4. Crear VPC/subredes/SGs y luego `enable_mwaa=true`.
5. Subir DAGs a `s3://<bucket>/dags/` y requirements a `s3://<bucket>/requirements/requirements.txt`.
6. Migrar el warehouse local a Redshift, adaptar `src/db.py` y las credenciales.
7. Configurar schedule, alertas, secretos y backend de estado remoto.

## Seguridad

- No hay claves AWS en el repositorio.
- El bucket no es público y rechaza tráfico sin TLS.
- IAM no usa `s3:*` sobre `*`: está limitado al bucket; el execution role y el deployment role están separados.
- `deployment_principal_arn` vacío cae al root de la cuenta solo para laboratorio; en producción debe apuntar a un rol de CI concreto.
- `force_destroy=false` evita borrados accidentales del bucket.
- `enable_mwaa=false` evita costos involuntarios en la demo; las alarmas publican a SNS sin suscriptores hasta que el equipo los configure.

## Costos aproximados

Supuestos de laboratorio en `us-east-1`, precios de referencia; verifica la calculadora de AWS. **No se aplicó nada.**

| Recurso | Supuesto | Aproximado |
|---|---|---:|
| S3 Standard | 10 GB + lifecycle | ~USD 0.23/mes + requests |
| CloudWatch Logs + alarmas | 1 GB ingerido, 30 días, 1–3 alarmas | ~USD 0.80/mes |
| MWAA `mw1.small` | 730 h, sin workers extra | ~USD 0.50/h ≈ **USD 360/mes** |
| Redshift Serverless | 8 RPU mínimo, si se habilita | ~USD 2.88/h |
| VPC/NAT | No incluido | Puede superar el costo de S3/CloudWatch |

La demo local no crea recursos AWS. `enable_mwaa=false` y `force_destroy=false` son deliberados.

## Limpieza

Local (destructivo; borra volúmenes del proyecto):

```powershell
docker compose down -v
```

AWS, solo en una cuenta autorizada y con el mismo state:

```powershell
cd terraform
terraform state list
# Vaciar versiones y delete markers (bucket versionado):
aws s3api delete-objects --bucket <bucket> --delete "$(aws s3api list-object-versions --bucket <bucket> --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' --output json)"
aws s3api delete-objects --bucket <bucket> --delete "$(aws s3api list-object-versions --bucket <bucket> --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' --output json)"
# Luego:
terraform destroy -var="enable_mwaa=false"   # usa true si se aplicó con MWAA
```

`force_destroy=false` exige vaciar el bucket versionado antes de destruirlo; es una protección, no un paso omitible. El topic SNS debe tener una suscripción operativa (email/Slack/PagerDuty) para que las alarmas notifiquen.

## Diferencia con el entorno local

| Local | Terraform AWS |
|---|---|
| Moto S3 | S3 real |
| Airflow LocalExecutor | MWAA opcional |
| PostgreSQL | Redshift (fuera del alcance de este módulo) |
| `.env` | Secrets Manager/SSM |
| `docker compose` | Cuenta/región AWS |

El objetivo del módulo es demostrar criterio de IaC y mínimo privilegio, no producir un despliegue productivo sin las decisiones de red, costo y seguridad que corresponden a un equipo.
