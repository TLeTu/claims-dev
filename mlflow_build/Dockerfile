# Use a standard Python image
FROM python:3.9-slim

WORKDIR /app

# Install MLflow and required libraries for PostgreSQL backend and S3 artifact store
RUN pip install mlflow \
                psycopg2-binary \
                boto3

# Expose the port MLflow will run on
EXPOSE 5001

# Command to start the MLflow tracking server
# It uses environment variables for configuration, which will be passed from docker-compose.yml
CMD mlflow server \
    --backend-store-uri "postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}" \
    --default-artifact-root "s3://car-smart-claims/mlflow-artifacts" \
    --host "0.0.0.0" \
    --port "5001"