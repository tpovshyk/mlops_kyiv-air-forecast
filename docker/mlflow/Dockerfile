FROM ghcr.io/mlflow/mlflow:v3.16.1
# Драйвер PostgreSQL (бекенд) і клієнт S3 (артефакти в MinIO) не входять у базовий образ.
RUN pip install --no-cache-dir "psycopg2-binary==2.9.10" "boto3==1.35.99"
