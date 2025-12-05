FROM apache/airflow:2.8.1-python3.11

USER root

# Update Docker CLI to version compatible with Docker Engine API 1.44
RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-26.1.3.tgz -o docker.tgz && \
    tar xzvf docker.tgz && \
    cp docker/docker /usr/bin/docker && \
    rm -rf docker docker.tgz && \
    chmod +x /usr/bin/docker

USER airflow
