#!/usr/bin/env bash
# Create the Phase 1 Azure resources in ONE resource group (delete the group = stop all charges):
#   rg-atliq-capstone
#     ├─ SQL server atliq-sql-<sfx> + database atliq_commerce (Basic tier, ~$5/month)
#     ├─ storage account atliqlake<sfx> (ADLS Gen2, container "lakehouse")
#     └─ monthly budget alert (INR 1,500) emailed to the signed-in account
# The SQL admin password is read with hidden input and written ONLY to python/.env (gitignored).
# Safe to re-run: existing resources are left as they are.
set -euo pipefail
cd "$(dirname "$0")/.."

RG=rg-atliq-capstone
LOC=${LOC:-centralindia}
[ -f azure.env ] || echo "SFX=$(openssl rand -hex 3)" > azure.env
source azure.env
SQL=atliq-sql-$SFX; ST=atliqlake$SFX; ADF=atliq-adf-$SFX; DB=atliq_commerce; ADMIN=atliq_admin
cat > azure.env <<VARS
SFX=$SFX
RG=$RG
LOC=$LOC
SQL=$SQL
DB=$DB
ST=$ST
ADF=$ADF
VARS

if [ ! -f python/.env ]; then
  echo "Choose a SQL admin password: 12+ chars, upper + lower + digit + one of ! @ % ^ * - _ (no quotes, #, \$ or spaces)."
  read -rs -p "SQL admin password: " PW; echo
  read -rs -p "Again: " PW2; echo
  [ "$PW" = "$PW2" ] || { echo "Passwords differ"; exit 1; }
  umask 077
  printf 'AZ_SQL_SERVER=%s.database.windows.net\nAZ_SQL_DB=%s\nAZ_SQL_USER=%s\nAZ_SQL_PASSWORD=%s\n' "$SQL" "$DB" "$ADMIN" "$PW" > python/.env
  echo "wrote python/.env (gitignored)"
else
  PW=$(grep '^AZ_SQL_PASSWORD=' python/.env | cut -d= -f2-)
fi

for ns in Microsoft.Sql Microsoft.Storage Microsoft.DataFactory; do
  az provider register --namespace "$ns" --wait -o none
done

echo "== resource group"; az group create -n "$RG" -l "$LOC" -o none
echo "== SQL server $SQL (2-4 min)"
az sql server show -g "$RG" -n "$SQL" -o none 2>/dev/null || \
  az sql server create -g "$RG" -n "$SQL" -l "$LOC" -u "$ADMIN" -p "$PW" -o none
MYIP=$(curl -s https://api.ipify.org)
az sql server firewall-rule create -g "$RG" -s "$SQL" -n my-laptop --start-ip-address "$MYIP" --end-ip-address "$MYIP" -o none
az sql server firewall-rule create -g "$RG" -s "$SQL" -n allow-azure-services --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0 -o none
echo "== database $DB (Basic)"
az sql db show -g "$RG" -s "$SQL" -n "$DB" -o none 2>/dev/null || \
  az sql db create -g "$RG" -s "$SQL" -n "$DB" --service-objective Basic --backup-storage-redundancy Local -o none

echo "== storage $ST (ADLS Gen2)"
az storage account show -g "$RG" -n "$ST" -o none 2>/dev/null || \
  az storage account create -g "$RG" -n "$ST" -l "$LOC" --sku Standard_LRS --kind StorageV2 --hns true \
     --min-tls-version TLS1_2 --allow-blob-public-access false -o none
KEY=$(az storage account keys list -g "$RG" -n "$ST" --query "[0].value" -o tsv)
az storage fs create -n lakehouse --account-name "$ST" --account-key "$KEY" -o none 2>/dev/null || true

echo "== budget alert (INR 1,500/month, email at 80%)"
SUB=$(az account show --query id -o tsv); ME=$(az account show --query user.name -o tsv)
START=$(date +%Y-%m-01)
az rest --method put -o none \
  --url "https://management.azure.com/subscriptions/$SUB/providers/Microsoft.Consumption/budgets/atliq-capstone?api-version=2023-05-01" \
  --body "{\"properties\":{\"category\":\"Cost\",\"amount\":1500,\"timeGrain\":\"Monthly\",
           \"timePeriod\":{\"startDate\":\"${START}T00:00:00Z\"},
           \"notifications\":{\"at80\":{\"enabled\":true,\"operator\":\"GreaterThan\",\"threshold\":80,
                                     \"contactEmails\":[\"$ME\"]}}}}" \
  && echo "budget set" || echo "budget not set (add it in Cost Management later; not blocking)"

echo
echo "DONE. Names (not secrets) saved in azure.env:"
cat azure.env
