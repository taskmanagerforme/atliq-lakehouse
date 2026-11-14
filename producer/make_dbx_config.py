"""Build the Databricks 00_config notebook from producer/.env, ready to import.

Writes databricks/local/00_config.py (gitignored). Import it into the same Databricks
folder as 01_kafka_stream_processing, which loads it with %run ./00_config.
Usage: python3 ~/cap2/producer/make_dbx_config.py
"""
from pathlib import Path

HERE = Path(__file__).parent
env = dict(
    line.split("=", 1)
    for line in (HERE / ".env").read_text().splitlines()
    if "=" in line and not line.startswith("#")
)
out = HERE.parent / "databricks" / "local" / "00_config.py"
out.parent.mkdir(exist_ok=True)
out.write_text(
    "# Databricks notebook source\n"
    "# Kafka credentials, loaded by 01_kafka_stream_processing via %run ./00_config\n"
    f'KAFKA_BOOTSTRAP = "{env["KAFKA_BOOTSTRAP"]}"\n'
    f'KAFKA_API_KEY = "{env["KAFKA_API_KEY"]}"\n'
    f'KAFKA_API_SECRET = "{env["KAFKA_API_SECRET"]}"\n'
)
print(f"wrote {out}")
