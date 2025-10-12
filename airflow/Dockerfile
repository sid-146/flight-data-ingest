FROM python:3.12-slim

# Setting env vars
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=2.10.3
ENV PYTHON_VERSION=3.12
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Installing system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    # For trigger spark jobs
    openjdk-21-jre-headless \ 
    build-essential \
    curl \
    git \
    # header files needed if building psycopg2 from source
    libpq-dev \
    procps \
    # For handling zipping for spark
    zip \
    unzip \
    && rm -rf /var/lib/api/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh 
ENV PATH="/root/.local/bin:${PATH}"


# Check if group/user already exist if not then create
RUN getent group airflow || groupadd -r -g 50000 airflow 
RUN id airflow || useradd -r -u 50000 -g airflow -d ${AIRFLOW_HOME} -s /bin/bash airflow

# Creating directories with proper ownership
RUN mkdir -p ${AIRFLOW_HOME} && \ 
    mkdir -p ${AIRFLOW_HOME}/dags && \
    mkdir -p ${AIRFLOW_HOME}/logs && \
    mkdir ${AIRFLOW_HOME}/plugins && \
    chown -R 50000:50000 ${AIRFLOW_HOME} && \
    chmod -R 755 ${AIRFLOW_HOME}


RUN pip install --no-cache-dir "apache-airflow[postgres,spark]==${AIRFLOW_VERSION}" \
    --constraint ${CONSTRAINT_URL} \
    psycopg2-binary

# Todo move this airflow to pyproject toml to handle single file dependency file.
COPY ./airflow/airflow-requirements.txt /tmp/airflow-requirements.txt
# COPY ./airflow/alembic.ini /opt/airflow/alembic.ini
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt

COPY ./airflow/entrypoint.sh /entrypoint.sh
COPY ../pyproject.toml /pyproject.toml

RUN uv pip compile pyproject.toml -o requirements.txt
RUN pip install -r requirements.txt


RUN chmod +x /entrypoint.sh

USER airflow
WORKDIR ${AIRFLOW_HOME}

EXPOSE 9099
# For logs 8793
EXPOSE 8793

CMD ["/entrypoint.sh"]
