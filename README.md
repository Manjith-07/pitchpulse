# PitchPulse

PitchPulse is a cricket analytics platform that combines a historical batch pipeline with a live streaming pipeline into a single API and dashboard. Historical ball-by-ball data (via Cricsheet) is loaded into a Postgres warehouse and transformed with dbt into analytics-ready tables, which are used to train a win probability model. During a live match, a streaming pipeline (Kafka + a Python producer/consumer) ingests real-time updates, maintains current match state in Redis, and applies the trained model to produce a live-updating win probability. A FastAPI service exposes both the historical stats and the live match state, and a Streamlit dashboard visualizes both.

## Status

This project is being built phase by phase. Current progress:

- [x] Phase 0: environment and project setup
- [x] Phase 1: batch pipeline (Postgres warehouse, loader, Airflow DAG, dbt models)
- [ ] Phase 2: streaming pipeline (Kafka producer/consumer, Redis state)
- [ ] Phase 3: win probability model (feature engineering, training, evaluation)
- [ ] Phase 4: serving API and dashboard (FastAPI, Streamlit)
- [ ] Phase 5: testing, CI, documentation, polish
- [ ] Phase 6 (optional): cloud deployment

## Architecture

```
Historical archive (Cricsheet)          Live match feed (cricket API)
        |                                        |
        v                                        v
  Batch pipeline                          Streaming pipeline
  (Airflow schedules a                    (Python poller -> Kafka
   loader script + dbt)                    topic -> Python consumer)
        |                                        |
        v                                        v
  Warehouse (Postgres,                    Real-time store (Redis,
  analytics tables)                       current match state)
        |                                        |
        +------------------+---------------------+
                            v
                     Serving API (FastAPI)
                            |
                            v
                       Dashboard (Streamlit)
```

The win probability model is trained offline from warehouse data and loaded inside the streaming consumer, so every live update also produces a live probability written to Redis alongside the match state.

## Tech stack

- **Language**: Python 3.11+
- **Historical data**: Cricsheet (free, no auth)
- **Live data**: a free-tier cricket API (e.g. CricAPI)
- **Message queue**: Apache Kafka (KRaft mode, single broker)
- **Orchestration**: Apache Airflow
- **Transformation**: dbt (dbt-postgres)
- **Warehouse**: PostgreSQL 16
- **Real-time store**: Redis 7
- **ML**: scikit-learn, pandas, joblib
- **API**: FastAPI + Uvicorn
- **Dashboard**: Streamlit
- **Containerization**: Docker + Docker Compose
- **CI**: GitHub Actions

## Setup

Setup instructions will be filled in as each phase is completed. The full walkthrough (fresh clone to running dashboard) will land in Phase 5.

### Phase 0 (done)

1. Copy `.env.example` to `.env` and fill in real values, especially `LIVE_CRICKET_API_KEY` (sign up at cricapi.com or a RapidAPI cricket API for a free key).
2. Historical IPL data is downloaded from Cricsheet into `data/ipl_raw/` (gitignored — re-download with the command below on a fresh clone):
   ```bash
   wget https://cricsheet.org/downloads/ipl_male_json.zip -P data/
   unzip data/ipl_male_json.zip -d data/ipl_raw
   ```

### Phase 1 (done): batch pipeline

Postgres (warehouse) and Airflow (orchestration) now run via Docker Compose. On
Linux/WSL, set `AIRFLOW_UID` in `.env` to your host user's UID (`id -u`) first —
Airflow's container needs to write to the bind-mounted `dags/`, `dbt/`, and
`data/` folders, and this keeps file ownership matching.

```bash
docker compose up -d postgres airflow
```

This brings up:
- **Postgres** on `localhost:5432`, with two databases in one instance: `pitchpulse`
  (the warehouse) and `airflow` (Airflow's own metadata), created automatically
  by `postgres/init-airflow-db.sh` on first startup.
- **Airflow** (single container, `airflow standalone` mode: webserver + scheduler
  + triggerer) on `http://localhost:8080`. dbt runs from its own virtualenv baked
  into the Airflow image (`/opt/dbt_venv`), kept separate from Airflow's own
  Python dependencies since dbt-core and Airflow pin conflicting versions of
  several shared packages.

**Note on deviating from the spec:** rather than Airflow's official multi-container
quickstart (webserver, scheduler, triggerer, worker, flower, its own Postgres/Redis),
this project runs Airflow as a single container in `airflow standalone` mode
(LocalExecutor), pointed at the same Postgres instance as the warehouse. That
quickstart is built for Celery-based horizontal scaling, which is unnecessary
overhead for a single local DAG in a portfolio project; `standalone` still runs
the real webserver, scheduler, and triggerer processes, just in one container.

Log into the Airflow UI with `admin` / whatever password you set (see below),
find the `pitchpulse_batch_pipeline` DAG, and trigger it. It runs two tasks:
1. `load_raw_data` — parses every Cricsheet JSON file in `data/ipl_raw/` into
   `raw_matches` and `raw_deliveries` in Postgres (safe to re-run; upserts skip
   rows that already exist).
2. `run_dbt` — runs `dbt build`, which creates the staging views and mart
   tables (including `match_state_features`, the training table for Phase 3)
   and runs all dbt tests.

The DAG can also be triggered from the CLI:
```bash
docker compose exec airflow airflow dags unpause pitchpulse_batch_pipeline
docker compose exec airflow airflow dags trigger pitchpulse_batch_pipeline
```

On first startup, Airflow creates an `admin` account with a random password
written to `/opt/airflow/standalone_admin_password.txt` inside the container
(`docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt`).
If that file isn't there, set a known password instead:
```bash
docker compose exec airflow airflow users reset-password --username admin --password <your-password>
```

## Repository structure

See `docs/architecture.md` for the full architecture writeup. Top-level layout:

```
pitchpulse/
├── ingestion/       # batch loader + streaming producer/consumer
├── dags/            # Airflow DAGs
├── dbt/             # dbt project (staging + mart models)
├── ml/              # feature engineering, model training, evaluation
├── api/             # FastAPI service
├── dashboard/       # Streamlit dashboard
├── tests/           # unit tests
└── docs/            # architecture notes
```
