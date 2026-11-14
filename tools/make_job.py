"""Create (or update) the nightly Databricks job and export its definition to jobs/nightly_job.json.

  silver   01_bronze_to_silver  (run_date = the trigger date: reads bronze/<table>/ingest_date=<run_date>)
  gold_dbt dbt build on the SQL warehouse (staging + star schema + 37 tests)
  audit    03_run_audit         (appends row counts + revenue to atliq.silver.run_audit)

Schedule: daily 01:00 Asia/Kolkata. Failure alert: email to the workspace user.
Run:  uv run tools/make_job.py
"""
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"])
me = w.current_user.me().user_name
folder = f"/Users/{me}/atliq_capstone"
warehouse_id = os.environ["DATABRICKS_HTTP_PATH"].rstrip("/").rsplit("/", 1)[-1]
NAME = "atliq_nightly_sync"

settings = dict(
    name=NAME,
    tasks=[
        jobs.Task(task_key="silver",
                  notebook_task=jobs.NotebookTask(
                      notebook_path=f"{folder}/01_bronze_to_silver", source=jobs.Source.WORKSPACE,
                      base_parameters={"run_date": "{{job.trigger.time.iso_date}}"})),
        jobs.Task(task_key="gold_dbt", depends_on=[jobs.TaskDependency(task_key="silver")],
                  environment_key="dbt_env",
                  dbt_task=jobs.DbtTask(commands=["dbt build"], project_directory=f"{folder}/atliq_gold",
                                        source=jobs.Source.WORKSPACE, warehouse_id=warehouse_id,
                                        catalog="atliq", schema="gold")),
        jobs.Task(task_key="audit", depends_on=[jobs.TaskDependency(task_key="gold_dbt")],
                  notebook_task=jobs.NotebookTask(
                      notebook_path=f"{folder}/03_run_audit", source=jobs.Source.WORKSPACE,
                      base_parameters={"run_id": "{{job.run_id}}",
                                       "run_date": "{{job.trigger.time.iso_date}}",
                                       "start_time": "{{job.start_time.iso_datetime}}"})),
    ],
    environments=[jobs.JobEnvironment(environment_key="dbt_env", spec=compute.Environment(
        environment_version="2", dependencies=["dbt-databricks>=1.10,<2.0"]))],
    schedule=jobs.CronSchedule(quartz_cron_expression="0 0 1 * * ?", timezone_id="Asia/Kolkata",
                               pause_status=jobs.PauseStatus.UNPAUSED),
    email_notifications=jobs.JobEmailNotifications(on_failure=[me]),
    max_concurrent_runs=1,
)

existing = [j for j in w.jobs.list(name=NAME)]
if existing:
    job_id = existing[0].job_id
    w.jobs.reset(job_id=job_id, new_settings=jobs.JobSettings(**settings))
    print("updated job", job_id)
else:
    job_id = w.jobs.create(**settings).job_id
    print("created job", job_id)

export = w.jobs.get(job_id).settings.as_dict()
export["email_notifications"] = {"on_failure": ["<workspace user email>"]}   # keep the address out of git
(ROOT / "jobs").mkdir(exist_ok=True)
(ROOT / "jobs" / "nightly_job.json").write_text(json.dumps(export, indent=2).replace(me, "<user>"))
print("exported -> jobs/nightly_job.json")
print(f"open: {w.config.host.rstrip('/')}/jobs/{job_id}")
