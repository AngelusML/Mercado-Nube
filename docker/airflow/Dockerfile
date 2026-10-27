FROM apache/airflow:2.10.5

USER airflow

COPY --chown=airflow:root docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
