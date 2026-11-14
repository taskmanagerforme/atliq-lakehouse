"""
AtliQ Phase 2 (LEARNER STARTER) — Streaming Ops DAG
====================================================
The stream never stops — but the SCHEDULED work around it is your job:
data-quality gate, table maintenance, daily rollup. Build an hourly DAG:

    check_fresh_events  ->  optimize_tables  ->  refresh_daily_summary

Setup:
1. In docker-compose.yaml:  _PIP_ADDITIONAL_REQUIREMENTS: apache-airflow-providers-databricks
   then: docker compose down && docker compose up -d
2. Airflow UI -> Admin -> Connections -> +
   Conn Id: databricks_default | Type: Databricks
   Host: https://<workspace>.cloud.databricks.com | Password: <PAT token>
   (Azure Databricks: Host is https://adb-<workspace-id>.<n>.azuredatabricks.net)
3. Paste your SQL warehouse HTTP path below.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

SQL_WAREHOUSE_HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/<warehouse-id>")  # Serverless Starter Warehouse

# The DQ gate's window. Graded value = 2 HOURS. For the failing-run screenshot,
# switch to 5 MINUTES temporarily, then set it back.
FRESHNESS_WINDOW = "2 HOURS"

default_args = {
    "owner": "atliq-data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="atliq_streaming_ops",
    start_date=datetime(2026, 8, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["atliq", "phase2", "streaming"],
) as dag:

    # TODO 1 — Data-quality gate.
    # A DatabricksSqlOperator that FAILS when no events landed recently.
    # Hint: Databricks SQL has assert_true(condition, message) — write a SELECT
    # that asserts COUNT(*) > 0 over silver_order_events for the last 2 hours.
    # A DQ check that can never fail is worth zero marks — you must be able to
    # demo it failing when the producer is stopped.
    # assert_true raises an error -> the SQL fails -> the task fails, with OUR message.
    check_fresh_events = DatabricksSqlOperator(
        task_id="check_fresh_events",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql=f"""
            SELECT assert_true(
                COUNT(*) > 0,
                'DQ FAIL: no events in silver_order_events in the last {FRESHNESS_WINDOW}'
            ) AS fresh_check
            FROM atliq.streaming.silver_order_events
            WHERE event_ts >= current_timestamp() - INTERVAL {FRESHNESS_WINDOW}
        """,
    )

    # TODO 2 — Table maintenance.
    # Streaming writes create many small files. Run OPTIMIZE on
    # silver_order_events and gold_revenue_5min.
    optimize_tables = DatabricksSqlOperator(
        task_id="optimize_tables",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql=[
            "OPTIMIZE atliq.streaming.silver_order_events",
            "OPTIMIZE atliq.streaming.gold_revenue_5min",
        ],
    )

    # TODO 3 — Daily rollup.
    # CREATE OR REPLACE atliq.streaming.gold_daily_summary: per event_date —
    # orders_placed, orders_paid, orders_cancelled, revenue (from paid events).
    # Hint: COUNT_IF() and a CASE inside SUM().
    refresh_daily_summary = DatabricksSqlOperator(
        task_id="refresh_daily_summary",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql="""
            CREATE OR REPLACE TABLE atliq.streaming.gold_daily_summary AS
            SELECT
                to_date(event_ts)                                                  AS event_date,
                COUNT_IF(event_type = 'order_placed')                              AS orders_placed,
                COUNT_IF(event_type = 'payment_received')                          AS orders_paid,
                COUNT_IF(event_type = 'order_cancelled')                           AS orders_cancelled,
                ROUND(SUM(CASE WHEN event_type = 'payment_received'
                               THEN order_amount ELSE 0 END), 2)                   AS revenue
            FROM atliq.streaming.silver_order_events
            GROUP BY to_date(event_ts)
        """,
    )

    check_fresh_events >> optimize_tables >> refresh_daily_summary
