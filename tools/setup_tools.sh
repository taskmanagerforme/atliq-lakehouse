#!/usr/bin/env bash
# One-time local setup for Phase 1 Azure work (Ubuntu 24.04 / Mint 22):
# ODBC Driver 18 (simulator + OLTP loader) and the Azure CLI from Microsoft's apt repo, then sign in.
set -euo pipefail
cd "$(dirname "$0")/.."
if ! dpkg -s msodbcsql18 >/dev/null 2>&1; then
  curl -sSLO https://packages.microsoft.com/config/ubuntu/24.04/packages-microsoft-prod.deb
  sudo dpkg -i packages-microsoft-prod.deb && rm -f packages-microsoft-prod.deb
  sudo apt-get update || true
  sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc
fi
echo "ODBC: $(odbcinst -q -d | tr '\n' ' ')"

# remove the broken uv-installed copy (old 2.0.67, runs in whatever venv is active)
uv tool uninstall azure-cli >/dev/null 2>&1 || true
if ! dpkg -s azure-cli >/dev/null 2>&1; then
  sudo mkdir -p /etc/apt/keyrings
  curl -sLS https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor | sudo tee /etc/apt/keyrings/microsoft.gpg >/dev/null
  echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/repos/azure-cli/ noble main" \
    | sudo tee /etc/apt/sources.list.d/azure-cli.list >/dev/null
  sudo apt-get update || true
  sudo apt-get install -y azure-cli
fi
hash -r
/usr/bin/az version --query '"azure-cli"' -o tsv
/usr/bin/az login
