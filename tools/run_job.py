"""Trigger the nightly job now (same as "Run now"), wait for it, then print the run_audit history.
run_date = the trigger date, so today's ADF batch (bronze/<table>/ingest_date=<today>) is merged.
Run:  uv run tools/run_job.py
"""
import os
from datetime import timedelta
from pathlib import Path

from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"])
job = next(iter(w.jobs.list(name="atliq_nightly_sync")))
print(f"running atliq_nightly_sync (job {job.job_id}), about 4 min ...")
run = w.jobs.run_now(job_id=job.job_id).result(timeout=timedelta(minutes=30))
print("run", run.run_id, run.state.result_state.value if run.state.result_state else run.state.life_cycle_state.value)
for t in run.tasks or []:
    print(f"  {t.task_key:9s} {t.state.result_state.value if t.state.result_state else t.state.life_cycle_state.value}")

wh = os.environ["DATABRICKS_HTTP_PATH"].rstrip("/").rsplit("/", 1)[-1]
res = w.statement_execution.execute_statement(warehouse_id=wh, wait_timeout="50s", statement=
    "SELECT run_date, finished_at, silver_orders, silver_order_items, silver_payments, fact_rows, "
    "total_gross_revenue FROM atliq.silver.run_audit ORDER BY finished_at")
cols = [c.name for c in res.manifest.schema.columns]
print("\n" + " | ".join(cols))
for r in res.result.data_array or []:
    print(" | ".join(str(x) for x in r))
