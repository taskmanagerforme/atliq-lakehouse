#!/usr/bin/env bash
# M2: deploy the ADF factory + metadata-driven pipeline, land the CSVs and clickstream in
# lakehouse/landing/, run the pipeline once and wait for it. Safe to re-run.
#   bash tools/adf_up.sh            deploy + upload + run
#   bash tools/adf_up.sh run        run only (pipeline already deployed)
set -euo pipefail
cd "$(dirname "$0")/.."
source azure.env
SUB=$(az account show --query id -o tsv)
KEY=$(az storage account keys list -g "$RG" -n "$ST" --query "[0].value" -o tsv)
API="https://management.azure.com/subscriptions/$SUB/resourceGroups/$RG/providers/Microsoft.DataFactory/factories/$ADF"

if [ "${1:-}" != "run" ]; then
  echo "== landing files (2 CSVs + 184 clickstream)"
  az storage blob upload-batch -d lakehouse --destination-path landing/csv -s csv \
     --account-name "$ST" --account-key "$KEY" --overwrite -o none
  az storage blob upload-batch -d lakehouse --destination-path landing/clickstream -s clickstream \
     --account-name "$ST" --account-key "$KEY" --overwrite -o none
  echo "== deploy factory $ADF + pipeline (1-2 min)"
  PW=$(grep '^AZ_SQL_PASSWORD=' python/.env | cut -d= -f2-)
  az deployment group create -g "$RG" -n adf-atliq --template-file adf/arm_template.json -o none \
     --parameters factoryName="$ADF" sqlServer="$SQL" sqlDb="$DB" sqlUser=atliq_admin \
                  storageAccount="$ST" sqlPassword="$PW"
fi

echo "== run pl_nightly_ingest"
RUN=$(az rest --method post --url "$API/pipelines/pl_nightly_ingest/createRun?api-version=2018-06-01" --query runId -o tsv)
echo "runId $RUN"
while :; do
  S=$(az rest --method get --url "$API/pipelineruns/$RUN?api-version=2018-06-01" --query status -o tsv)
  echo "  $(date +%H:%M:%S) $S"
  case "$S" in Queued|InProgress|Canceling) sleep 15 ;; *) break ;; esac
done

echo "== activities"
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ); FROM=$(date -u -d '-3 hours' +%Y-%m-%dT%H:%M:%SZ)
az rest --method post --url "$API/pipelineruns/$RUN/queryActivityruns?api-version=2018-06-01" \
  --body "{\"lastUpdatedAfter\":\"$FROM\",\"lastUpdatedBefore\":\"$NOW\"}" \
  --query "value[].{activity:activityName,status:status,rows:output.rowsCopied,files:output.filesWritten,error:error.message}" -o table

echo "== bronze in ADLS"
az storage fs file list -f lakehouse --path bronze --account-name "$ST" --account-key "$KEY" \
  --query "[?!isDirectory && !contains(name,'clickstream/')].{file:name,bytes:contentLength}" -o table
[ "$S" = "Succeeded" ]
