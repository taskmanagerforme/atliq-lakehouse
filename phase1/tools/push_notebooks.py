"""Import databricks/*.py as notebooks and copy the dbt project (atliq_gold/) as workspace files
into /Users/<you>/atliq_capstone/  (overwrites). profiles.yml is left out on purpose: the job's
dbt task generates its own profile from the SQL warehouse, so no token is ever stored there.
Run:  uv run tools/push_notebooks.py
"""
import base64
import os
from pathlib import Path

from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"])
folder = f"/Users/{w.current_user.me().user_name}/atliq_capstone"
w.workspace.mkdirs(folder)
for nb in sorted((ROOT / "databricks").glob("*.py")):
    w.workspace.import_(path=f"{folder}/{nb.stem}", format=ImportFormat.SOURCE, language=Language.PYTHON,
                        content=base64.b64encode(nb.read_bytes()).decode(), overwrite=True)
    print("imported", f"{folder}/{nb.stem}")

dbt_root = ROOT / "atliq_gold"
skip = {"target", "logs", "dbt_packages"}
n = 0
for f in dbt_root.rglob("*"):
    rel = f.relative_to(dbt_root)
    if not f.is_file() or rel.parts[0] in skip or f.name in {"profiles.yml", ".user.yml"}:
        continue
    dest = f"{folder}/atliq_gold/{rel.as_posix()}"
    w.workspace.mkdirs(dest.rsplit("/", 1)[0])
    with f.open("rb") as fh:
        w.workspace.upload(dest, fh, format=ImportFormat.RAW, overwrite=True)
    n += 1
print(f"copied {n} dbt files -> {folder}/atliq_gold")
