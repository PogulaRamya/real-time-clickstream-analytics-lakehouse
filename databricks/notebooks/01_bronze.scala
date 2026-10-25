// Databricks notebook source
// DBR 15.4 LTS, dedicated access mode, Unity Catalog enabled.
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
Seq("catalog", "stream_name", "region", "checkpoint_root").foreach(k => dbutils.widgets.text(k, ""))
val catalog = dbutils.widgets.get("catalog")
require(catalog.matches("[A-Za-z_][A-Za-z0-9_]*"), "Invalid catalog")
val checkpoint = dbutils.widgets.get("checkpoint_root")
require(checkpoint.nonEmpty, "A durable checkpoint root is required")
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sql(s"CREATE SCHEMA IF NOT EXISTS $catalog.bronze")
val source = spark.readStream.format("kinesis")
  .option("streamName", dbutils.widgets.get("stream_name"))
  .option("region", dbutils.widgets.get("region"))
  .option("initialPosition", "trim_horizon")
  .load()
// Authentication comes from the cluster's instance profile, never inline keys.
val query = source.select(col("data").cast("string").as("raw_json"),
    col("partitionKey").as("partition_key"), col("sequenceNumber").as("sequence_number"),
    current_timestamp().as("ingested_at"))
  .writeStream.format("delta").outputMode("append")
  .option("checkpointLocation", s"$checkpoint/bronze")
  .trigger(Trigger.ProcessingTime("10 seconds"))
  .toTable(s"$catalog.bronze.events")
query.awaitTermination()
