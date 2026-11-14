#!/usr/bin/env bash
# Allow this laptop's mobile-network range (IP rotates on every connection) to reach Azure SQL.
# Usage: bash tools/fw.sh <start-ip> <end-ip>  -> allow that range (e.g. your mobile carrier /16)
#        bash tools/fw.sh 1.2.3.4               -> just that IP
set -euo pipefail
cd "$(dirname "$0")/.."
source azure.env
S=${1:?start ip}; E=${2:-$1}
az sql server firewall-rule create -g "$RG" -s "$SQL" -n my-laptop --start-ip-address "$S" --end-ip-address "$E" -o none
az sql server firewall-rule list -g "$RG" -s "$SQL" --query "[].{rule:name,from:startIpAddress,to:endIpAddress}" -o table
