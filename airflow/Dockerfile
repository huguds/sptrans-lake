# =====================================================================
# Dockerfile: Airflow + Spark 3.5.1 + Delta Lake + Hadoop AWS (S3A)
# =====================================================================
FROM ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.9.3-python3.11}

# ---------------------------------------------------------------------
# Instala Java e dependências básicas
# ---------------------------------------------------------------------
USER root
RUN apt-get update && \
    apt-get install -y wget curl gcc python3-dev openjdk-17-jdk && \
    apt-get clean

# Define JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# ---------------------------------------------------------------------
# Instala dependências Python (Airflow + Kafka)
# ---------------------------------------------------------------------
USER airflow

COPY ./requirements.txt . 
RUN pip install -r requirements.txt
# ---------------------------------------------------------------------
# Copia DAGs e plugins do projeto
# ---------------------------------------------------------------------
COPY ./dags /opt/airflow/dags
COPY ./plugins /opt/airflow/plugins