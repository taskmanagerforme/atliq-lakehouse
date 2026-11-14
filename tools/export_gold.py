"""Export the 5 Gold tables from Databricks to Parquet for upload into a Fabric Lakehouse.
Free Edition cannot expose Gold on ADLS for a OneLake shortcut, so we copy the (small) tables.
Run:  uv run tools/export_gold.py      -> upload/fabric/<table>.parquet
"""
import os
from pathlib import Path

import pyarrow.parquet as pq
from databricks import sql
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
OUT = ROOT / "upload" / "fabric"
OUT.mkdir(parents=True, exist_ok=True)
TABLES = ["fact_sales", "dim_customer", "dim_product", "dim_date", "fact_web_sessions"]

with sql.connect(server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/"),
                 http_path=os.environ["DATABRICKS_HTTP_PATH"],
                 access_token=os.environ["DATABRICKS_TOKEN"]) as conn, conn.cursor() as cur:
    for t in TABLES:
        cur.execute(f"SELECT * FROM atliq.gold.{t}")
        tbl = cur.fetchall_arrow()
        pq.write_table(tbl, OUT / f"{t}.parquet")
        print(f"{t:18s} {tbl.num_rows:>7,} rows  {len(tbl.schema)} cols")
print("files in", OUT.relative_to(ROOT))
