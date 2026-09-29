# Generic runtime for both producer.py and consumer.py. The actual code is
# bind-mounted in at runtime (see docker-compose.yml) rather than copied at
# build time, so editing a script doesn't require an image rebuild -- the
# same pattern used for the Airflow container in phase 1.
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
