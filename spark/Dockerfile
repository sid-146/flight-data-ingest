# Base Image of python
FROM python:3.12

# Env variables
ENV SPARK_USER_HOME=/home/spark
ENV SPARK_VERSION=3.5.7
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64


# Todo Create user a spark user and group with correct permission
RUN getent group spark || groupadd -r -g 50000 spark 
RUN id spark || useradd -r -u 50000 -g spark -d ${SPARK_USER_HOME} -s /bin/bash spark

USER root

# Create folders

# dependencies install
RUN apt-get update
RUN apt-get install -y \
    openjdk-21-jre-headless \
    curl \
    zip \
    tini \
    && rm -rf /var/lib/apt/lists/*


# Commented is archive
#  curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
RUN pwd
# Download and store to spark_home
# Create the SPARK_HOME directory and then download/extract into it
RUN mkdir -p ${SPARK_HOME} && \
    curl -fSL "https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xvzf - -C ${SPARK_HOME} --strip-components=1

# Create ENV TEMP_FOLDER=/temp before uncommenting following lines.
# ENV TEMP_FOLDER=/temp
# RUN mkdir ${TEMP_FOLDER}
# RUN curl -fSL "https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz "-o ${TEMP_FOLDER}/spark.tgz
# RUN tar -xvzf --strip-components=1 ${TEMP_FOLDER}/spark.tgz -C ${SPARK_HOME}
# RUN rm ${TEMP_FOLDER}/spark.tgz



# Install UV
RUN curl -LsSf https://astral.sh/uv/install.sh | sh 
ENV PATH="/root/.local/bin:${PATH}"

# Installing Source Code dependencies
COPY ../pyproject.toml ${SPARK_USER_HOME}/pyproject.toml
WORKDIR ${SPARK_USER_HOME}
RUN uv pip compile ${SPARK_USER_HOME}/pyproject.toml -o requirements.txt
RUN uv pip install -r requirements.txt --system


# Change ownership of the folders
RUN chown -R spark:spark ${SPARK_USER_HOME}
RUN chown -R spark:spark ${SPARK_HOME}

COPY ./spark/entrypoint.sh ${SPARK_USER_HOME}/entrypoint.sh

RUN sed -i 's/\r$//' ${SPARK_USER_HOME}/entrypoint.sh
RUN chmod +x ${SPARK_USER_HOME}/entrypoint.sh

USER spark
WORKDIR ${SPARK_USER_HOME}

EXPOSE 8080 7077 4040

ENTRYPOINT ["tini", "--", "/home/spark/entrypoint.sh"]
