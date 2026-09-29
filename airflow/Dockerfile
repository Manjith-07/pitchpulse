# Custom Airflow image that can also run dbt.
#
# dbt-core's dependency versions (click, jinja2, markupsafe, protobuf, ...)
# conflict with Airflow's own pinned versions badly enough that pip's
# resolver backtracks through dozens of releases and still fails. Rather than
# fight that, dbt gets its own isolated virtualenv baked into the image, with
# no shared dependencies with Airflow at all. The `run_dbt` DAG task calls
# /opt/dbt_venv/bin/dbt directly.
#
# The loader script only needs psycopg2-binary, which the base Airflow image
# already ships, so no extra install is needed for it.
FROM apache/airflow:2.10.4-python3.11

USER root
RUN python -m venv /opt/dbt_venv && \
    /opt/dbt_venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/dbt_venv/bin/pip install --no-cache-dir dbt-postgres~=1.8 && \
    chown -R airflow: /opt/dbt_venv
USER airflow
