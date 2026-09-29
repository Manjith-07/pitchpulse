# Generic runtime; the actual code is bind-mounted in at runtime (see
# docker-compose.yml), same pattern used for the Airflow and streaming
# containers, so editing a route doesn't require an image rebuild.
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
