# AtliQ Commerce — Real-Time Speed Lane (Capstone Phase 2)

The nightly batch lakehouse (Phase 1) answers yesterday's questions. This speed lane streams order
events through Kafka into Delta tables, so revenue per 5 minutes is visible while the shop is running,
and an hourly Airflow DAG guards and maintains it.

```
order_event_producer.py ──> Confluent Kafka topic atliq.orders.events (key = order_id)
                                   │
         Databricks Structured Streaming (01_kafka_stream_processing.py)
                                   │
   Bronze  bronze_order_events     every message as received (+ partition, offset, kafka_ts)
   Silver  silver_order_events     parsed, broken records -> silver_quarantine, replays de-duplicated
   Gold    gold_revenue_5min       5-minute windows, 10-minute watermark, append mode
                                   │
   Airflow DAG atliq_streaming_ops (@hourly)
      check_fresh_events  -> FAILS if Silver got no events in the last 2 hours (assert_true;
                           set to 5 minutes for the failure demo)
      optimize_tables     -> OPTIMIZE Silver + Gold (streaming writes many small files)
      refresh_daily_summary -> gold_daily_summary (placed / paid / cancelled / revenue per day)
```

## Status

| Part | Evidence |
|---|---|
| Producer → Kafka, keyed by order_id (same key → same partition) | `docs/confluent_messages.png` (179 messages) |
| Bronze → Silver: quarantine + de-duplication | `docs/silver_drops_first_run.png`: 179 in Bronze, 4 quarantined, 168 in Silver |
| Gold 5-minute windows, appearing after the watermark | `docs/gold_first_run.png`, `docs/gold_late_windows_1446.png` |
| Airflow green run | `docs/airflow_green_run.png` |
| Airflow DQ gate failing when the producer is stopped | `docs/airflow_dq_fail.jpg`, `docs/airflow_failed_run_grid.png` |
| Write-up (4 questions) | [`WRITEUP.md`](WRITEUP.md) |

## Platform notes

- **Databricks Free Edition (serverless):** always-on triggers are not available, so every stream
  uses `.trigger(availableNow=True)` (process everything new since the checkpoint, then stop) and is
  re-run while the producer is running. On a full workspace the same code runs with
  `.trigger(processingTime="30 seconds")`. Checkpoints live in a Unity Catalog volume.
- **Confluent Cloud** basic cluster for Kafka; **Airflow** runs locally in Docker.

## Run it

Needs [uv](https://docs.astral.sh/uv/).

```bash
uv sync
cp producer/.env.example producer/.env          # fill in Kafka bootstrap + API key/secret
uv run producer/order_event_producer.py         # send order events
uv run producer/make_dbx_config.py              # writes databricks/local/00_config.py (gitignored)
# Databricks: import databricks/01_kafka_stream_processing.py (+ your 00_config), run all
# Airflow: put airflow/dags/ in your dags folder, set DATABRICKS_HTTP_PATH and the
#          databricks_default connection, then trigger atliq_streaming_ops
```

## Repo layout

| Path | What |
|---|---|
| `producer/order_event_producer.py` | Generates order events (incl. replays, broken and late ones) and sends them keyed by order_id |
| `producer/make_dbx_config.py` | Builds the Databricks config notebook from `.env` |
| `databricks/00_config.example.py` | Template for the config notebook (real one is gitignored) |
| `databricks/01_kafka_stream_processing.py` | Bronze, Silver (quarantine, dedupe), Gold (windows) streams |
| `airflow/dags/atliq_streaming_ops_dag.py` | Hourly DQ gate, OPTIMIZE, daily summary |
| `docs/` | Evidence screenshots |

## Secrets

No secrets are committed: `producer/.env`, `databricks/local/` (the real config notebook with the
Kafka key) and `airflow.cfg` are gitignored; the DAG reads the warehouse path from `DATABRICKS_HTTP_PATH`.
