# PitchPulse: complete build specification

This document is a full implementation spec for PitchPulse, a cricket analytics platform combining a historical batch pipeline and a live streaming pipeline, merged into one API with a live-updating win probability model. It is written to be handed to an autonomous coding agent (Claude Code) and followed phase by phase.

## 0. How to use this document

Implement this project in the phase order below (0 through 5). Do not skip ahead. After finishing each phase, verify it against that phase's "Definition of done" checklist before moving to the next one. If a version number or image tag mentioned below is outdated by the time this is built, use the current stable equivalent and note the substitution in the README. Ask the user before making any architectural decision not covered in this document; otherwise, proceed autonomously using the decisions specified here.

The user (MJ) is a beginner at data engineering specifically, though comfortable with Python, SQL, and basic Docker. Prefer clear, well-commented code over clever code. Every phase should leave the project in a runnable state.

## 1. Project summary

PitchPulse ingests historical cricket match data in a scheduled batch pipeline, ingests live match data in a real-time streaming pipeline, trains a win probability model on the historical data, applies that model live during streaming, and exposes everything through one API and one dashboard.

## 2. Goals and success criteria

- A scheduled batch pipeline loads historical cricket data into a Postgres warehouse and transforms it into clean analytics tables using dbt
- A streaming pipeline ingests live match updates through Kafka and maintains current match state in Redis
- A win probability model, trained on historical ball-by-ball data, produces a live-updating probability during a real match
- A single FastAPI service exposes both historical stats and live match state plus win probability
- A dashboard (Streamlit) displays live scores with a live win probability chart, and a historical stats explorer
- The entire stack runs locally via Docker Compose with one command
- Data quality tests run automatically via dbt tests and CI on every push
- The final repository is clean enough to link directly from a resume or portfolio site

## 3. Architecture

Two parallel data paths converge into one serving layer:

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

The win probability model is trained offline from warehouse data (phase 3) and loaded inside the streaming consumer, so every live update also produces a live probability written to Redis alongside the match state.

## 4. Complete tech stack

| Component | Choice | Notes |
|---|---|---|
| Language | Python 3.11+ | All scripts, DAGs, API, model training |
| Historical data | Cricsheet | Free, no auth, no rate limit |
| Live data | CricAPI or a RapidAPI cricket API | Free tier, requires an API key |
| Message queue | Apache Kafka (KRaft mode, single broker, no ZooKeeper) | Use the official `apache/kafka` image if available at build time, otherwise `bitnami/kafka` with KRaft enabled |
| Orchestration | Apache Airflow | Use Airflow's official docker-compose quick-start as the base (`https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml`), adapted to mount our DAGs folder and add a Postgres connection to the warehouse |
| Transformation | dbt (dbt-postgres adapter) | Run via a Python virtual environment, not necessarily containerized |
| Warehouse | PostgreSQL 16 | One instance, two databases: `pitchpulse` (warehouse) and `airflow` (Airflow metadata) |
| Real-time store | Redis 7 | Stores current match state and win probability per match_id |
| ML | scikit-learn, pandas, joblib | Logistic regression baseline, gradient boosting as a stretch comparison |
| API | FastAPI + Uvicorn | Serves both historical and live endpoints |
| Dashboard | Streamlit | Live scoreboard tab and historical explorer tab |
| Containerization | Docker + Docker Compose | Runs the entire stack with one command |
| CI | GitHub Actions | Runs dbt tests and Python unit tests on every push |
| Data quality | dbt tests (not_null, unique, relationships) | Applied to every staging and mart model |

## 5. Repository structure

```
pitchpulse/
├── README.md
├── docker-compose.yml
├── .env.example
├── .gitignore
├── data/                                # gitignored, raw downloaded files
│   └── ipl_raw/
├── ingestion/
│   ├── batch/
│   │   ├── load_cricsheet.py            # parses JSON, loads raw tables into Postgres
│   │   └── requirements.txt
│   └── streaming/
│       ├── producer.py                  # polls live API, publishes to Kafka
│       ├── consumer.py                  # reads Kafka, computes state, writes to Redis
│       └── requirements.txt
├── dags/
│   └── cricsheet_batch_dag.py           # Airflow DAG: run loader then dbt
├── dbt/
│   └── pitchpulse/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       └── models/
│           ├── staging/
│           │   ├── stg_matches.sql
│           │   ├── stg_deliveries.sql
│           │   └── schema.yml           # tests for staging models
│           └── marts/
│               ├── player_career_stats.sql
│               ├── team_venue_performance.sql
│               ├── phase_wise_trends.sql
│               ├── match_state_features.sql   # ball-by-ball features for ML training
│               └── schema.yml           # tests for mart models
├── ml/
│   ├── feature_engineering.py
│   ├── train_win_probability.py
│   ├── evaluate.py
│   └── model/
│       ├── win_probability_model.pkl
│       └── feature_list.json
├── api/
│   ├── main.py
│   ├── routers/
│   │   ├── historical.py
│   │   └── live.py
│   ├── db.py                            # Postgres and Redis connection helpers
│   └── requirements.txt
├── dashboard/
│   ├── app.py
│   └── requirements.txt
├── tests/
│   ├── test_load_cricsheet.py
│   └── test_feature_engineering.py
├── .github/
│   └── workflows/
│       └── ci.yml
└── docs/
    └── architecture.md                  # copy of section 3 of this document, plus a diagram image
```

## 6. Environment variables (.env.example)

```
POSTGRES_USER=pitchpulse
POSTGRES_PASSWORD=pitchpulse
POSTGRES_DB=pitchpulse
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

AIRFLOW_DB=airflow

REDIS_HOST=redis
REDIS_PORT=6379

KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=live-match-events

LIVE_CRICKET_API_KEY=your_key_here
LIVE_CRICKET_API_BASE_URL=https://api.cricapi.com/v1
```

Never commit the real `.env` file, only `.env.example`.

## 7. Data sources, exact details

**Historical: Cricsheet**
- IPL matches (JSON): `https://cricsheet.org/downloads/ipl_male_json.zip` (1,243 matches, small download)
- All matches, every format ever recorded (JSON): `https://cricsheet.org/downloads/all_json.zip` (about 94 MB, 22,000+ matches), use this later if IPL alone isn't enough signal for the model
- Format reference: `https://cricsheet.org/format/json/`
- Each zip contains one JSON file per match with an `info` block (teams, venue, dates, toss, outcome, player_of_match, season) and an `innings` array, each innings containing `overs`, each over containing `deliveries` with batter, bowler, non_striker, runs (batter/extras/total), and an optional `wickets` array on dismissal balls.

**Live: a cricket API**
- CricAPI (`cricapi.com`) or a cricket API on RapidAPI, both have free tiers
- Requires signup for an API key, stored in `.env` as `LIVE_CRICKET_API_KEY`
- Free tiers are request-limited per day, so the producer should only poll during matches actually being tested, not continuously

## 8. Phase 0: environment and project setup

Steps (skip any already completed):

1. Confirm Docker is available: `docker --version` and `docker compose version`
2. Create the repository structure exactly as in section 5
3. `git init`, create `.gitignore` containing at minimum: `data/`, `*.zip`, `.env`, `__pycache__/`, `*.pyc`, `ml/model/*.pkl`, `.venv/`
4. Download IPL data: `wget https://cricsheet.org/downloads/ipl_male_json.zip -P data/` then `unzip data/ipl_male_json.zip -d data/ipl_raw`
5. Sign up for a free live cricket API key and store it in a local `.env` file (copy from `.env.example`)
6. Write the initial `README.md` with a one-paragraph project description and a "setup" section to be filled in as later phases complete

**Definition of done:** repository exists with the full folder structure, IPL data is downloaded and unzipped locally, `.env` exists locally (not committed), first git commit is made.

## 9. Phase 1: batch pipeline

### 9.1 Raw schema (Postgres, database `pitchpulse`)

```sql
CREATE TABLE raw_matches (
    match_id TEXT PRIMARY KEY,
    match_type TEXT,
    season TEXT,
    match_date DATE,
    venue TEXT,
    city TEXT,
    team1 TEXT,
    team2 TEXT,
    toss_winner TEXT,
    toss_decision TEXT,
    winner TEXT,
    win_by_type TEXT,       -- 'runs' or 'wickets'
    win_by_margin INTEGER,
    player_of_match TEXT
);

CREATE TABLE raw_deliveries (
    match_id TEXT REFERENCES raw_matches(match_id),
    innings_number INTEGER,
    batting_team TEXT,
    over_number INTEGER,
    ball_number INTEGER,
    batter TEXT,
    bowler TEXT,
    non_striker TEXT,
    runs_batter INTEGER,
    runs_extras INTEGER,
    runs_total INTEGER,
    extras_type TEXT,
    is_wicket BOOLEAN DEFAULT FALSE,
    wicket_kind TEXT,
    player_dismissed TEXT,
    PRIMARY KEY (match_id, innings_number, over_number, ball_number)
);
```

### 9.2 `ingestion/batch/load_cricsheet.py`

- Accepts a directory of Cricsheet JSON files as input
- Parses each file's `info` block into one row of `raw_matches`
- Parses each innings/over/delivery into rows of `raw_deliveries`
- Uses `psycopg2` or `SQLAlchemy` with `ON CONFLICT DO NOTHING` upserts so the script is safely re-runnable
- Logs a summary at the end: matches processed, deliveries inserted, matches skipped due to parse errors (with reasons)

### 9.3 Airflow DAG (`dags/cricsheet_batch_dag.py`)

- DAG id: `pitchpulse_batch_pipeline`
- Schedule: daily (`@daily`) for demonstration purposes; note in a comment that a real cricket season would need a more specific schedule
- Task 1: `load_raw_data` — runs `load_cricsheet.py` via `BashOperator` or `PythonOperator`
- Task 2: `run_dbt` — runs `dbt build` against the `dbt/pitchpulse` project via `BashOperator`
- Task 2 depends on Task 1
- Add retries (2) with a reasonable retry delay on both tasks

### 9.4 dbt models

**Staging (`dbt/pitchpulse/models/staging/`)**
- `stg_matches.sql`: selects from `raw_matches`, casts types, standardizes team name casing
- `stg_deliveries.sql`: selects from `raw_deliveries`, adds a computed `is_boundary` flag (runs_batter in (4, 6)) and a `phase` column (`powerplay` for overs 1-6, `middle` for overs 7-15, `death` for overs 16-20)
- `schema.yml`: add `not_null` and `unique` tests on primary keys, `not_null` on team names and match_id foreign keys

**Marts (`dbt/pitchpulse/models/marts/`)**
- `player_career_stats.sql`: aggregates by batter and by bowler across all matches — total runs, balls faced, strike rate, dismissals, wickets taken, economy rate
- `team_venue_performance.sql`: win percentage by team, grouped by venue
- `phase_wise_trends.sql`: average run rate by phase (powerplay/middle/death), grouped by team and season
- `match_state_features.sql`: the most important mart, reconstructs ball-by-ball match state for the second (chasing) innings only — for every delivery, compute `runs_needed`, `balls_remaining`, `wickets_in_hand`, `current_run_rate`, `required_run_rate`, and a `chasing_team_won` label (1 or 0) derived from the match's final outcome. This is the training table for phase 3.
- `schema.yml`: tests on the marts, especially `not_null` on `chasing_team_won` in `match_state_features`

**Definition of done for phase 1:** `docker compose up postgres airflow` runs, the Airflow UI shows `pitchpulse_batch_pipeline` succeeding end to end, and querying `match_state_features` in Postgres returns populated rows with no nulls in the label column.

## 10. Phase 2: streaming pipeline

### 10.1 Kafka topic

- Topic name: `live-match-events`
- One partition is sufficient for a portfolio project (note in docs that production would need more partitions for parallelism)
- Message schema (JSON):
```json
{
  "match_id": "string",
  "timestamp": "ISO8601 string",
  "innings_number": 1,
  "over_number": 12,
  "ball_number": 4,
  "batting_team": "string",
  "bowling_team": "string",
  "current_score": 98,
  "wickets_lost": 3,
  "target": 175,
  "balls_remaining": 46
}
```

### 10.2 `ingestion/streaming/producer.py`

- Polls the live cricket API every 10 to 15 seconds for a given `match_id` passed as a command line argument
- Transforms the API's response into the message schema above
- Publishes to the `live-match-events` Kafka topic using `kafka-python` or `confluent-kafka`
- Stops cleanly when the API reports the match has ended
- Handles API errors and rate limits with exponential backoff, does not crash the process on a single failed request

### 10.3 `ingestion/streaming/consumer.py`

- Subscribes to `live-match-events`
- On each message, computes `current_run_rate` and `required_run_rate` (when a target exists)
- Loads the trained model from `ml/model/win_probability_model.pkl` (built in phase 3) and the feature list from `ml/model/feature_list.json`
- Computes a live win probability from the message's current state using the loaded model
- Writes a single JSON blob to Redis at key `match:{match_id}:state` containing the latest score, run rates, and win probability, with a TTL of a few hours so stale matches expire automatically
- Also appends the same event to a Postgres table `live_events_log` (optional but recommended) so live matches can be replayed or analyzed after the fact

**Definition of done for phase 2:** with Kafka and Redis running, running the producer against a real or simulated match ID results in the consumer's Redis key updating every 10 to 15 seconds with a live-changing win probability.

## 11. Phase 3: win probability model

### 11.1 `ml/feature_engineering.py`

- Reads the `match_state_features` mart from Postgres
- Splits into train/test by match_id (never split mid-match, to avoid leakage) — for example, an 80/20 split of unique match_ids
- Saves the feature list (column names used for training) to `ml/model/feature_list.json`

### 11.2 `ml/train_win_probability.py`

- Trains a baseline `LogisticRegression` on features: `runs_needed`, `balls_remaining`, `wickets_in_hand`, `current_run_rate`, `required_run_rate`
- Trains a second model, `GradientBoostingClassifier` (or XGBoost if available), on the same features
- Evaluates both on the held-out test set using AUC and log loss
- Saves whichever model performs better as `ml/model/win_probability_model.pkl` using `joblib`
- Prints a short evaluation report to the console, and writes it to `ml/model/evaluation_report.txt`

### 11.3 `ml/evaluate.py`

- A standalone script that loads the saved model and prints a calibration check: for a handful of hand-picked match states (e.g. "needs 10 off 6 balls with 2 wickets in hand"), print the predicted win probability, so MJ can sanity check the model makes cricketing sense before relying on it live

**Definition of done for phase 3:** a saved model file exists, its AUC is meaningfully better than 0.5 (ideally above 0.75), and the sanity check script produces probabilities that make intuitive cricket sense (e.g. needing 5 off the last ball should show a low probability, needing 5 off 12 balls with wickets in hand should show a high one).

## 12. Phase 4: serving API and dashboard

### 12.1 FastAPI endpoints (`api/main.py` and `api/routers/`)

`historical.py`:
- `GET /historical/player/{player_name}` — returns career batting and bowling stats from `player_career_stats`
- `GET /historical/team/{team_name}/venues` — returns win percentage by venue from `team_venue_performance`
- `GET /historical/phase-trends?team=&season=` — returns phase-wise run rate trends

`live.py`:
- `GET /live/matches` — lists currently tracked match_ids (scans Redis keys matching `match:*:state`)
- `GET /live/match/{match_id}` — returns the full current state JSON stored in Redis for that match, including win probability
- Returns a 404 with a clear message if no state exists for that match_id

Root:
- `GET /health` — simple liveness check, returns `{"status": "ok"}`

Use Pydantic response models for every endpoint so the API is self-documenting via FastAPI's automatic `/docs` page.

### 12.2 Dashboard (`dashboard/app.py`, Streamlit)

- Sidebar: a dropdown to pick a currently live match (populated from `/live/matches`), or a mode toggle to "Historical explorer"
- **Live tab**: current score, current run rate, required run rate, and a live-updating line chart of win probability over the balls seen so far (poll the API every few seconds using `st.rerun` on a timer, or a manual refresh button if simplicity is preferred for the first version)
- **Historical tab**: a text input for player name showing career stats, a team + venue win percentage table, and a phase-wise trends bar chart

**Definition of done for phase 4:** with the full stack running, opening the dashboard shows real live match data updating during an actual match, and the historical tab returns real numbers for any player who appears in the IPL dataset.

## 13. Phase 5: testing, CI, documentation, polish

1. Write `docker-compose.yml` bringing up every service (postgres, kafka, redis, airflow, api, dashboard) with one command: `docker compose up -d`
2. Add `.github/workflows/ci.yml`: on every push, spin up a Postgres service container, install dbt, run `dbt build`, then run `pytest` on the `tests/` folder
3. Write unit tests for `load_cricsheet.py` (parsing correctness on a sample match file) and `feature_engineering.py` (correct feature computation on a known input)
4. Finish the `README.md`: project description, the architecture diagram from section 3, full setup instructions from a fresh clone to a running dashboard, and a short demo GIF or screenshot
5. Optional stretch: deploy the dashboard and API to a free tier host (Render or Railway) and link it live from the README

**Definition of done for phase 5:** a fresh clone of the repository, following only the README, results in a fully running system with one `docker compose up -d` plus the documented one-time setup commands (data download, dbt run, model training).

## 14. Optional phase 6: cloud deployment (stretch, not required)

- Deploy the API and dashboard to a free tier service (Render, Railway, or an AWS free-tier EC2 instance)
- Keep Kafka, Postgres, Redis, and Airflow running locally or in Docker on the same free-tier instance if resources allow, or document clearly in the README that the live pipeline is intended to be demoed locally while the dashboard/API are the parts deployed
- Add the live link to the README and to MJ's resume/portfolio

## 15. Resume bullet points (for reference, not to be built)

- Built an end to end data pipeline combining batch and streaming architectures using Kafka, Airflow, and dbt, processing historical and live cricket match data
- Trained and deployed a win probability model served in real time through a streaming pipeline, updating predictions on live match events
- Containerized the full stack with Docker Compose and implemented CI based data quality testing with GitHub Actions

## 16. Appendix: docker-compose.yml skeleton

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  kafka:
    image: apache/kafka:latest
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      # complete KRaft env vars per the image's documentation at build time

  api:
    build: ./api
    ports:
      - "8000:8000"
    depends_on:
      - postgres
      - redis
    env_file: .env

  dashboard:
    build: ./dashboard
    ports:
      - "8501:8501"
    depends_on:
      - api
    env_file: .env

volumes:
  pgdata:
```

Airflow is intentionally left out of this skeleton. Use Airflow's official docker-compose.yaml as a separate base and merge or run it alongside this file, since Airflow's own compose setup handles several services (webserver, scheduler, triggerer, init) that are easy to get wrong by hand.

## 17. Build order summary (for Claude Code)

1. Phase 0: scaffold repo, download IPL data, set up `.env`
2. Phase 1: raw schema, loader script, Airflow DAG, dbt staging and marts
3. Phase 2: Kafka topic, producer, consumer, Redis state
4. Phase 3: feature engineering, model training, evaluation
5. Phase 4: FastAPI endpoints, Streamlit dashboard
6. Phase 5: tests, CI, README, final polish
7. Phase 6 (optional): cloud deployment

Confirm each phase's definition of done before starting the next.
