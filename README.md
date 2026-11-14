# AtliQ Commerce — Batch Lakehouse (Capstone Phase 1)

A nightly batch pipeline that copies AtliQ's operational (OLTP) data into a separate analytics
lakehouse, cleans it, and models it into a star schema, so analytics never slows the live shop.

```
Azure SQL (OLTP) ──ADF──> Bronze (raw Parquet/CSV/JSON) ──Databricks──> Silver (clean, MERGE)
                                                                │
                                                     dbt ──> Gold (star schema + web funnel)
                                                                │
                                                      nightly job 01:00 IST + run audit
```

## Status

| Area | State |
|---|---|
| Bronze → Silver (cleaning, dq_log, MERGE, quarantine) | ✅ done, all §7.7 checkpoints match (28/28) |
| Gold with dbt (5 tables, 7 views, 37 tests) | ✅ `dbt build` PASS=49 WARN=0 ERROR=0 |
| Nightly job (silver → gold_dbt → audit), failure email | ✅ scheduled 01:00 Asia/Kolkata, green run |
| Idempotency (run twice → same numbers) | ✅ proof in `docs/idempotency_proof.jpg` |
| Azure SQL OLTP load + simulator | ⏳ scripts ready (`tools/azure_up.sh`, `tools/load_oltp.py`), blocked on Azure subscription |
| ADF metadata-driven copy | ⏳ pending (same blocker) |
| CI (GitHub Actions dbt build) | ⏳ pending |
| Fabric dashboard | ⏳ pending |

### Why the platform is split (hybrid)

Transformation runs on **Databricks Free Edition** (Unity Catalog, serverless, SQL warehouse).
Free Edition cannot mount ADLS, so until ADF is live the Bronze layer is produced locally from the
starter package by `tools/seed_to_bronze.py` (same folder layout ADF writes:
`bronze/<table>/ingest_date=YYYY-MM-DD/`) and uploaded to a Unity Catalog volume.
The Azure side (Azure SQL + ADF → ADLS Bronze) plugs in at the Bronze folder; nothing downstream changes.

## Repo layout

| Path | What |
|---|---|
| `sql/` | OLTP schema, seed data, `etl.control_table` + watermark procedure (starter package) |
| `python/daily_order_simulator.py` | Adds new/updated orders to Azure SQL (starter package) |
| `csv/`, `clickstream/` | Supplier price list, marketing spend, web clickstream JSON (starter package) |
| `tools/seed_to_bronze.py` | Seed SQL → typed Parquet Bronze, partitioned by `ingest_date` |
| `tools/upload.py` | Uploads Bronze to `/Volumes/atliq/raw/landing/bronze` |
| `tools/push_notebooks.py` | Imports the notebooks + dbt project into the workspace |
| `tools/make_job.py` | Creates the nightly job and exports `jobs/nightly_job.json` |
| `tools/azure_up.sh`, `tools/load_oltp.py` | Azure SQL + ADLS setup and OLTP load with count checks |
| `databricks/01_bronze_to_silver.py` | Cleaning rules, `dq_log`, MERGE into Silver, quiet-night guard |
| `databricks/02_checkpoints.py` | The 28 §7.7 checkpoint queries |
| `databricks/03_run_audit.py` | Appends one row per run to `atliq.silver.run_audit` |
| `atliq_gold/` | dbt project: staging views, Gold star schema, tests |
| `jobs/nightly_job.json` | Exported job definition (user email masked) |
| `docs/` | Job run and idempotency screenshots |

## Run it

Needs [uv](https://docs.astral.sh/uv/). Copy `.env.example` to `.env` and fill in the workspace values.

```bash
uv sync
uv run tools/seed_to_bronze.py      # build Bronze from the starter package
uv run tools/upload.py              # upload to the UC volume
uv run tools/push_notebooks.py      # notebooks + dbt project into the workspace
uv run tools/make_job.py            # create the nightly job
cd atliq_gold && uv run --env-file ../.env dbt build   # dbt locally
```

## Key results (checkpoints, §7.7)

- Customers 1,000 real in 10 cities; products 60 in 6 categories; orders 9,961 after test-order removal.
- `fact_sales` 22,959 rows. Payments 8,183 totalling 32,424,661.00 = gross revenue of non-cancelled orders.
- Returned 4,574,944 → net 27,849,717.
- Clickstream: 426 bad events quarantined, 23,193 sessions; funnel 13,309 → 7,391 → 4,442 → 3,446.

## Idempotency

Silver uses `MERGE` on business keys and skips a night with no new Bronze batch
(the "quiet night" guard); Gold is rebuilt by dbt. Two consecutive job runs give identical
row counts and revenue (see `docs/idempotency_proof.jpg` and `atliq.silver.run_audit`).

## Secrets

No secrets are committed. `.env` and `python/.env` are gitignored; dbt's `profiles.yml` reads
only `env_var()`; the job export masks the user email.
