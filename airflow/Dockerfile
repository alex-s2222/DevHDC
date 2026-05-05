FROM apache/airflow:2.10.5-python3.11


USER root

RUN apt update && apt install -y tzdata

USER airflow

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__DEFAULT_TIMEZONE=UTC

WORKDIR ${AIRFLOW_HOME}

COPY --chown=airflow:airflow ../requirements.txt ./


RUN pip3 install -r requirements.txt