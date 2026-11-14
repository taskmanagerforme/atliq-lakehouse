"""M1: run the 7 OLTP scripts against Azure SQL, in order, each to completion, then check the counts.
Reads AZ_SQL_* from python/.env (same file the simulator uses). Splits each script on GO lines,
exactly as SSMS / the VS Code MSSQL extension do.
Run:  uv run tools/load_oltp.py
"""
import os
import re
import sys
import time
from pathlib import Path

import pyodbc
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "python" / ".env")
SCRIPTS = ["01_schema_ddl.sql", "02_insert_customers.sql", "03_insert_products.sql", "04_insert_orders.sql",
           "05_insert_order_items.sql", "06_insert_payments.sql", "07_etl_control_table.sql"]
EXPECTED = {"customers": 1008, "products": 60, "orders": 10000, "order_items": 23155, "payments": 8320}

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER=tcp:{os.environ['AZ_SQL_SERVER']},1433;DATABASE={os.environ['AZ_SQL_DB']};"
    f"UID={os.environ['AZ_SQL_USER']};PWD={os.environ['AZ_SQL_PASSWORD']};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;", autocommit=True)
cur = conn.cursor()

only = sys.argv[1:]   # optional: run just some scripts, e.g. 07_etl_control_table.sql
for name in SCRIPTS:
    if only and name not in only:
        continue
    t = time.time()
    text = (ROOT / "sql" / name).read_text(encoding="utf-8-sig")
    batches = [b for b in re.split(r"^\s*GO\s*;?\s*$", text, flags=re.M | re.I) if b.strip()]
    for b in batches:
        cur.execute(b)
        while cur.nextset():
            pass
    print(f"{name:28s} {len(batches):3d} batches  {time.time() - t:5.1f}s")

rows = cur.execute(
    "SELECT 'customers', COUNT(*) FROM dbo.customers UNION ALL SELECT 'products', COUNT(*) FROM dbo.products "
    "UNION ALL SELECT 'orders', COUNT(*) FROM dbo.orders UNION ALL SELECT 'order_items', COUNT(*) FROM dbo.order_items "
    "UNION ALL SELECT 'payments', COUNT(*) FROM dbo.payments").fetchall()
print("\ntable         rows   expected")
for tbl, n in rows:
    print(f"{tbl:12s} {n:6d}   {EXPECTED[tbl]:6d}  {'OK' if n == EXPECTED[tbl] else 'MISMATCH'}")
print("\netl.control_table:")
for r in cur.execute("SELECT table_name, load_type, watermark_column, last_loaded_at FROM etl.control_table"):
    print("  ", tuple(r))
