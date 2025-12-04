FROM bitnami/spark:3.4.1

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip3 install -r requirements.txt

WORKDIR /app
COPY . .

RUN mkdir -p /app/data /app/results
RUN chown -R 1001:1001 /app
