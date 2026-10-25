FROM apache/airflow:2.10.5-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir \
    requests==2.32.5 \
    pyspark==3.5.5