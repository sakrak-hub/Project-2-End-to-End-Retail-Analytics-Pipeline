FROM apache/airflow:3.0.1

USER root

USER airflow

RUN pip install --no-cache-dir "apache-airflow[amazon]" 