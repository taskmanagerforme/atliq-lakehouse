#!/usr/bin/env bash
# Bridge ADLS -> Databricks Free Edition (it cannot read ADLS directly).
# Downloads what tonight's ADF run wrote (full snapshots + today's ingest_date batches)
# into upload/bronze/ and pushes those folders to the UC volume. CSVs/clickstream are unchanged, so skipped.
#   bash tools/adf_pull.sh              (today, UTC = ADF's run date)
#   bash tools/adf_pull.sh 2026-11-14
set -euo pipefail
cd "$(dirname "$0")/.."
source azure.env
D=${1:-$(date -u +%F)}
KEY=$(az storage account keys list -g "$RG" -n "$ST" --query "[0].value" -o tsv)
for f in customers products orders/ingest_date=$D order_items/ingest_date=$D payments/ingest_date=$D; do
  mkdir -p "upload/bronze/$f"
  az storage fs file download -f lakehouse -p "bronze/$f/part-00000.parquet" -d "upload/bronze/$f/part-00000.parquet" \
     --account-name "$ST" --account-key "$KEY" --overwrite -o none
  echo "pulled bronze/$f"
done
uv run tools/upload.py customers products orders order_items payments
echo "Now run the nightly job (Run now) -> run_date = $D"
