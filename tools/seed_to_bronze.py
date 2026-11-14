"""Stand-in for the ADF Copy step: turn the seed SQL inserts into a Bronze landing tree.

ADF would copy Azure SQL -> ADLS as typed Parquet. Without ADF, this script does the
same thing from the seed scripts, using the same folder layout ADF writes (guide §6.1):

  upload/bronze/customers/                         full snapshot (overwrite)
  upload/bronze/products/                          full snapshot (overwrite)
  upload/bronze/orders/ingest_date=<run_date>/     incremental batch
  upload/bronze/order_items/ingest_date=<run_date>/
  upload/bronze/payments/ingest_date=<run_date>/
  upload/bronze/supplier_price_list/   CSV as-is (text, typed in Silver)
  upload/bronze/marketing_spend/       CSV as-is
  upload/bronze/clickstream/           JSON Lines as-is (parsed in Silver)

Values are written exactly as given (messy text stays messy); only SQL types are applied.
Run:  uv run tools/seed_to_bronze.py
"""
import re
import shutil
import sys
from decimal import Decimal
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "upload" / "bronze"
RUN_DATE = sys.argv[1] if len(sys.argv) > 1 else "2026-08-31"   # seed batch = end of seed period

TS = pa.timestamp("us")
DEC = lambda p: pa.decimal128(p, 2)
TABLES = {  # file, load type, arrow schema in DDL column order
    "customers":   ("02_insert_customers.sql", "full", pa.schema([
        ("customer_id", pa.int32()), ("customer_name", pa.string()), ("email", pa.string()),
        ("city", pa.string()), ("signup_date", pa.date32()), ("updated_at", TS)])),
    "products":    ("03_insert_products.sql", "full", pa.schema([
        ("product_id", pa.int32()), ("product_name", pa.string()), ("category", pa.string()),
        ("unit_price", DEC(10)), ("updated_at", TS)])),
    "orders":      ("04_insert_orders.sql", "incremental", pa.schema([
        ("order_id", pa.int32()), ("customer_id", pa.int32()), ("order_date", pa.date32()),
        ("status", pa.string()), ("order_amount", DEC(12)), ("created_at", TS), ("updated_at", TS)])),
    "order_items": ("05_insert_order_items.sql", "incremental", pa.schema([
        ("order_item_id", pa.int32()), ("order_id", pa.int32()), ("product_id", pa.int32()),
        ("quantity", pa.int32()), ("item_price", DEC(10)), ("created_at", TS)])),
    "payments":    ("06_insert_payments.sql", "incremental", pa.schema([
        ("payment_id", pa.int32()), ("order_id", pa.int32()), ("amount", DEC(12)),
        ("method", pa.string()), ("paid_at", TS), ("updated_at", TS)])),
}
EXPECTED = {"customers": 1008, "products": 60, "orders": 10000, "order_items": 23155, "payments": 8320}

# one SQL literal: N'..' / '..' (with '' escapes), NULL, or a number
TOKEN = re.compile(r"N?'((?:[^']|'')*)'|(NULL)|(-?\d+(?:\.\d+)?)")
ROW = re.compile(r"^\s*\((.*)\)\s*[,;]?\s*$")


def parse_row(body: str) -> list:
    vals, pos = [], 0
    while pos < len(body):
        m = TOKEN.match(body, pos)
        if not m:
            raise ValueError(f"cannot parse near: {body[pos:pos+40]!r}")
        s, null, num = m.groups()
        vals.append(None if null else (s.replace("''", "'") if s is not None else num))
        pos = m.end()
        while pos < len(body) and body[pos] in ", ":
            pos += 1
    return vals


def coerce(v, typ):
    if v is None:
        return None
    if pa.types.is_integer(typ):
        return int(v)
    if pa.types.is_decimal(typ):
        return Decimal(v)
    if pa.types.is_date(typ):
        return date.fromisoformat(v)
    if pa.types.is_timestamp(typ):
        return datetime.fromisoformat(v)
    return v


def main():
    if OUT.exists():
        shutil.rmtree(OUT)
    for table, (fname, load, schema) in TABLES.items():
        rows = []
        for line in (ROOT / "sql" / fname).read_text(encoding="utf-8").splitlines():
            m = ROW.match(line)
            if m and not line.lstrip().upper().startswith("INSERT"):
                vals = parse_row(m.group(1))
                if len(vals) != len(schema):
                    raise ValueError(f"{table}: {len(vals)} values, expected {len(schema)}: {line[:80]}")
                rows.append([coerce(v, f.type) for v, f in zip(vals, schema)])
        assert len(rows) == EXPECTED[table], f"{table}: {len(rows)} rows, expected {EXPECTED[table]}"
        cols = list(zip(*rows))
        tbl = pa.table([pa.array(c, type=f.type) for c, f in zip(cols, schema)], schema=schema)
        dest = OUT / table / (f"ingest_date={RUN_DATE}" if load == "incremental" else "")
        dest.mkdir(parents=True, exist_ok=True)
        pq.write_table(tbl, dest / "part-00000.parquet")
        print(f"{table:12s} {tbl.num_rows:6d} rows -> {dest.relative_to(ROOT)}")

    for name in ("supplier_price_list", "marketing_spend"):
        dest = OUT / name
        dest.mkdir(parents=True)
        shutil.copy(ROOT / "csv" / f"{name}.csv", dest / f"{name}.csv")
    shutil.copytree(ROOT / "clickstream", OUT / "clickstream")
    n = len(list((OUT / "clickstream").glob("*.json")))
    print(f"CSVs copied as-is; clickstream {n} files copied as-is")


if __name__ == "__main__":
    main()
