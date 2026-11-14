# AtliQ Commerce Data Platform (Capstone)

Two lanes over the same e-commerce business, one folder each. Each folder has its own README, write-up,
code and evidence, and runs on its own (`cd phase1` or `cd phase2` before following its run steps).

| Folder | Lane | What it does | Start here |
|---|---|---|---|
| [`phase1/`](phase1) | Batch lakehouse | Azure SQL (OLTP) → ADF (metadata-driven, watermarks) → Bronze → Silver (PySpark, Delta MERGE, dq_log) → Gold star schema (dbt, 37 tests) → nightly job with run audit → Fabric dashboard; dbt CI on GitHub Actions | [README](phase1/README.md), [architecture write-up](phase1/docs/architecture.md), [DQ report](phase1/docs/dq_report.md) |
| [`phase2/`](phase2) | Real-time speed lane | Kafka producer (Confluent, keyed by order_id) → Structured Streaming Bronze / Silver (quarantine, de-duplication, watermark) / Gold 5-minute revenue windows → hourly Airflow DAG with a freshness gate | [README](phase2/README.md), [write-up](phase2/WRITEUP.md) |

CI (`.github/workflows/ci.yml`) builds and tests the Phase 1 dbt project on every pull request and push to main.
No secrets are committed in either folder; each has a `.env.example` and a `.gitignore`.
