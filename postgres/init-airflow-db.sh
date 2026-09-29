#!/bin/bash
# Runs once, on first container startup with an empty data volume. Our single
# Postgres instance hosts two databases: the one named by POSTGRES_DB (the
# pitchpulse warehouse, created automatically by the postgres image) and a
# second one for Airflow's own metadata, created here.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    SELECT 'CREATE DATABASE "$AIRFLOW_DB"'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$AIRFLOW_DB')\gexec
EOSQL
