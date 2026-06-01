FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    confluent-kafka==2.3.0 \
    fastavro==1.9.4 \
    psycopg2-binary==2.9.9 \
    pyiceberg[pyarrow,s3fs]==0.7.1 \
    pyarrow==15.0.2

COPY flink/ ./flink/
COPY schemas/ ./schemas/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
