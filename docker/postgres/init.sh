#!/usr/bin/env bash
set -euo pipefail

# Este script solo se ejecuta la primera vez que se crea el volumen.
# Crea las dos bases y dos usuarios: Airflow (metadata) y mn_app (warehouse).
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER airflow WITH PASSWORD '${AIRFLOW_DB_PASSWORD}';
    CREATE USER mn_app WITH PASSWORD '${WAREHOUSE_PASSWORD}';
    CREATE DATABASE airflow OWNER airflow;
    CREATE DATABASE mercado_nube OWNER mn_app;
    ALTER DATABASE airflow SET timezone TO 'UTC';
    ALTER DATABASE mercado_nube SET timezone TO 'UTC';
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
    GRANT ALL PRIVILEGES ON DATABASE mercado_nube TO mn_app;
EOSQL

echo "Bases airflow y mercado_nube creadas."
