# Databricks notebook source
"""
BPM Adapter Logs Streaming Ingestion
Continuously reads JSON BPM adapter logs from Volume and writes to Delta table using Structured Streaming.
"""

# COMMAND ----------

import yaml
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Load YAML configuration
with open("../../config/common_config.yml", "r") as f:
    yaml_config = yaml.safe_load(f)

# Setup widgets for configuration parameters
dbutils.widgets.text("catalog", yaml_config["database"]["catalog"], "Catalog")
dbutils.widgets.text("schema", yaml_config["database"]["schema"], "Schema")
dbutils.widgets.text("volume_name", yaml_config["database"]["volume_name"], "Volume Name")

# Get widget values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_name = dbutils.widgets.get("volume_name")

# Build configuration dictionary
config = {
    "catalog": catalog,
    "schema": schema,
    "volume_name": volume_name,
    "bpm_adapter_logs_path": f"/Volumes/{catalog}/{schema}/{volume_name}/bpm_adapter",
    "bpm_adapter_checkpoint": f"/Volumes/{catalog}/{schema}/{volume_name}/_checkpoints/bpm_adapter",
    "bpm_adapter_table": f"{catalog}.{schema}.{yaml_config['log_types']['bpm_adapter']}",
    "max_files_per_trigger": yaml_config["streaming"]["max_files_per_trigger"],
    "trigger_processing_time": yaml_config["streaming"]["trigger_processing_time"],
}

print("Configuration loaded:")
print(f"  Source: {config['bpm_adapter_logs_path']}")
print(f"  Target: {config['bpm_adapter_table']}")
print(f"  Checkpoint: {config['bpm_adapter_checkpoint']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Schema

# COMMAND ----------

# Define the schema for BPM adapter logs
bpm_adapter_schema = StructType([
    StructField("context", StringType(), True),
    StructField("filename", StringType(), True),
    StructField("level", StringType(), True),
    StructField("log_uuid", StringType(), True),
    StructField("logger", StringType(), True),
    StructField("mdc", MapType(StringType(), StringType()), True),
    StructField("message", StructType([
        StructField("ConversationId", StringType(), True),
        StructField("ClientApp", StringType(), True),
        StructField("ClientDME2Lookup", StringType(), True),
        StructField("Mode", StringType(), True),
        StructField("Service", StringType(), True),
        StructField("CustomTag", StringType(), True),
        StructField("TrailMarks", ArrayType(StructType([
            StructField("Name", StringType(), True),
            StructField("Detail", StringType(), True),
            StructField("StartTime", StringType(), True),
            StructField("EndTime", StringType(), True),
            StructField("Complete", BooleanType(), True),
            StructField("ElapsedDuration", LongType(), True)
        ])), True),
        StructField("Cluster", StringType(), True),
        StructField("Token", StringType(), True),
        StructField("Vtier", StringType(), True),
        StructField("TransactionId", StringType(), True),
        StructField("InstanceName", StringType(), True),
        StructField("HostIPAddress", StringType(), True),
        StructField("ClientId", StringType(), True),
        StructField("StartTimeStamp", StringType(), True),
        StructField("Application", StringType(), True),
        StructField("ReqMsgSize", LongType(), True),
        StructField("RespMsgSize", LongType(), True),
        StructField("HostName", StringType(), True)
    ]), True),
    StructField("raw-message", StringType(), True),
    StructField("thread", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Streaming DataFrame

# COMMAND ----------

# Read streaming data from the volume
bpm_adapter_stream = (spark
    .readStream
    .format("json")
    .schema(bpm_adapter_schema)
    .option("maxFilesPerTrigger", config["max_files_per_trigger"])
    .load(config["bpm_adapter_logs_path"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Data

# COMMAND ----------

# Add ingestion timestamp and process the data
bpm_adapter_processed = (bpm_adapter_stream
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("log_timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
    .withColumn("conversation_id", col("message.ConversationId"))
    .withColumn("service", col("message.Service"))
    .withColumn("custom_tag", col("message.CustomTag"))
    .withColumn("cluster", col("message.Cluster"))
    .withColumn("vtier", col("message.Vtier"))
    .withColumn("transaction_id", col("message.TransactionId"))
    .withColumn("instance_name", col("message.InstanceName"))
    .withColumn("host_ip", col("message.HostIPAddress"))
    .withColumn("host_name", col("message.HostName"))
    .withColumn("client_id", col("message.ClientId"))
    .withColumn("application", col("message.Application"))
    .withColumn("req_msg_size", col("message.ReqMsgSize"))
    .withColumn("resp_msg_size", col("message.RespMsgSize"))
    # Extract trail marks information
    .withColumn("trail_marks_count", size(col("message.TrailMarks")))
    .withColumn("has_incomplete_trails", 
                when(col("trail_marks_count") > 0,
                     exists("message.TrailMarks", lambda x: x.Complete == False))
                .otherwise(False))
    # Calculate total elapsed time from trail marks
    .withColumn("total_elapsed_ms",
                when(col("trail_marks_count") > 0,
                     expr("aggregate(message.TrailMarks, CAST(0 AS BIGINT), (acc, x) -> acc + coalesce(x.ElapsedDuration, 0))"))
                .otherwise(None))
    # Parse start timestamp from first trail mark if available
    .withColumn("operation_start_timestamp",
                when(col("trail_marks_count") > 0,
                     to_timestamp(element_at(col("message.TrailMarks"), 1).StartTime, 
                                  "yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'"))
                .otherwise(None))
    .withColumn("operation_end_timestamp",
                when(col("trail_marks_count") > 0,
                     to_timestamp(element_at(col("message.TrailMarks"), 1).EndTime, 
                                  "yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'"))
                .otherwise(None))
    # Classify log
    .withColumn("is_error", when(col("level").isin("ERROR", "FATAL"), True).otherwise(False))
    .withColumn("is_timeout", col("has_incomplete_trails"))
    # High response time flag (> 30 seconds)
    .withColumn("is_high_response_time",
                when(col("total_elapsed_ms") > 30000, True).otherwise(False))
    # Extract MDC tracing info
    .withColumn("service_name", col("mdc")["service.name"])
    .withColumn("span_id", col("mdc")["span_id"])
    .withColumn("trace_id", col("mdc")["trace_id"])
    .withColumn("trace_flags", col("mdc")["trace_flags"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Table

# COMMAND ----------

# Ensure the database exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config['catalog']}.{config['schema']}")

# COMMAND ----------

# Write the streaming data to Delta table
query = (bpm_adapter_processed
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", config["bpm_adapter_checkpoint"])
    .trigger(processingTime=config["trigger_processing_time"])
    .option("mergeSchema", "true")
    .toTable(config["bpm_adapter_table"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor Stream

# COMMAND ----------

# Display stream status
print(f"Stream ID: {query.id}")
print(f"Stream Status: {query.status}")
print(f"\nStreaming BPM adapter logs from {config['bpm_adapter_logs_path']}")
print(f"Writing to table: {config['bpm_adapter_table']}")
print(f"Checkpoint location: {config['bpm_adapter_checkpoint']}")
print("\nStream is running. Monitor progress in the Spark UI.")

# COMMAND ----------

# Keep the stream running
query.awaitTermination()

