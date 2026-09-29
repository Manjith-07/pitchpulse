# Architecture

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

The win probability model is trained offline from warehouse data (phase 3) and loaded
inside the streaming consumer, so every live update also produces a live probability
written to Redis alongside the match state.

## Components

- **Batch pipeline**: an Airflow DAG (`dags/cricsheet_batch_dag.py`) runs a loader
  script (`ingestion/batch/load_cricsheet.py`) that parses Cricsheet JSON files into
  raw Postgres tables, then runs `dbt build` to transform them into staging and mart
  models.
- **Streaming pipeline**: `ingestion/streaming/producer.py` polls a live cricket API
  and publishes normalized events to the `live-match-events` Kafka topic.
  `ingestion/streaming/consumer.py` reads that topic, computes run rates, applies the
  trained win probability model, and writes current match state to Redis.
- **ML**: `ml/train_win_probability.py` trains a win probability model on the
  `match_state_features` dbt mart (ball-by-ball chase state) and saves it for the
  streaming consumer to load.
- **Serving layer**: `api/` (FastAPI) exposes historical stats (from Postgres marts)
  and live match state (from Redis). `dashboard/` (Streamlit) visualizes both.

A placeholder diagram image will be added here in Phase 5 alongside the finished
README.
