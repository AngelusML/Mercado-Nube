#!/usr/bin/env bash
set -euo pipefail

echo "Esperando que PostgreSQL acepte conexiones..."
until python -c "import socket; s=socket.create_connection(('postgres', 5432), 2); s.close()" 2>/dev/null; do
  sleep 2
done

echo "Migrando metadata de Airflow..."
airflow db migrate

echo "Bootstrap del warehouse local (DDL, semillas, bucket S3)..."
python -m src.bootstrap

echo "Creando usuario administrador local (si no existe)..."
airflow users create \
  --username "${AIRFLOW_ADMIN_USER}" \
  --password "${AIRFLOW_ADMIN_PASSWORD}" \
  --firstname Admin \
  --lastname Local \
  --role Admin \
  --email "${AIRFLOW_ADMIN_EMAIL}" || true

echo "Iniciando webserver y scheduler..."
airflow webserver --port 8080 &
WEBSERVER_PID=$!

airflow scheduler &
SCHEDULER_PID=$!

trap 'kill ${WEBSERVER_PID} ${SCHEDULER_PID} 2>/dev/null || true' TERM INT

wait -n "${WEBSERVER_PID}" "${SCHEDULER_PID}"
