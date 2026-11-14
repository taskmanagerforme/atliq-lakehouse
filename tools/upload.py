"""Push the local Bronze tree (upload/bronze/) into the Databricks Volume atliq.raw.landing.

Creates schema atliq.raw and volume landing if missing, then uploads every file,
keeping the folder layout (so ingest_date=... partitions survive).
Reads DATABRICKS_HOST / DATABRICKS_TOKEN from .env (never printed).
Run:  uv run tools/upload.py            (all of bronze/)
      uv run tools/upload.py orders customers   (just these source folders)
"""
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
CATALOG, SCHEMA, VOLUME = "atliq", "raw", "landing"
SRC = ROOT / "upload" / "bronze"
DEST = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/bronze"

w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"])

if not any(s.name == SCHEMA for s in w.schemas.list(catalog_name=CATALOG)):
    w.schemas.create(name=SCHEMA, catalog_name=CATALOG)
    print(f"created schema {CATALOG}.{SCHEMA}")
if not any(v.name == VOLUME for v in w.volumes.list(catalog_name=CATALOG, schema_name=SCHEMA)):
    from databricks.sdk.service.catalog import VolumeType
    w.volumes.create(catalog_name=CATALOG, schema_name=SCHEMA, name=VOLUME, volume_type=VolumeType.MANAGED)
    print(f"created volume {CATALOG}.{SCHEMA}.{VOLUME}")

bases = [SRC / a for a in sys.argv[1:]] or [SRC]
files = [p for b in bases for p in b.rglob("*") if p.is_file()]


def put(p: Path) -> None:
    with p.open("rb") as fh:
        w.files.upload(f"{DEST}/{p.relative_to(SRC).as_posix()}", fh, overwrite=True)


with ThreadPoolExecutor(8) as pool:
    for i, _ in enumerate(pool.map(put, files), 1):
        if i % 50 == 0 or i == len(files):
            print(f"uploaded {i}/{len(files)}")
print(f"done -> {DEST}")
