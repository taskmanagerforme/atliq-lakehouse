# Databricks notebook source
# Kafka credentials for 01_kafka_stream_processing (loaded there with %run ./00_config).
# Copy to a notebook named 00_config in the same Databricks folder and fill in your values;
# python3 producer/make_dbx_config.py builds it from producer/.env. Never commit the real one.
KAFKA_BOOTSTRAP = "pkc-xxxxx.region.provider.confluent.cloud:9092"
KAFKA_API_KEY = "YOUR_API_KEY"
KAFKA_API_SECRET = "YOUR_API_SECRET"
