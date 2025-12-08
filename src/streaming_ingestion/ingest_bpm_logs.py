# Databricks notebook source
"""
BPM Logs Streaming Ingestion
Continuously reads JSON BPM logs from Volume and writes to Delta table using Structured Streaming.
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
    "bpm_logs_path": f"/Volumes/{catalog}/{schema}/{volume_name}/bpm",
    "bpm_checkpoint": f"/Volumes/{catalog}/{schema}/{volume_name}/_checkpoints/bpm",
    "bpm_table": f"{catalog}.{schema}.bpm_logs",
    "max_files_per_trigger": yaml_config["streaming"]["max_files_per_trigger"],
    "trigger_processing_time": yaml_config["streaming"]["trigger_processing_time"],
}

print("Configuration loaded:")
print(f"  Source: {config['bpm_logs_path']}")
print(f"  Target: {config['bpm_table']}")
print(f"  Checkpoint: {config['bpm_checkpoint']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Schema

# COMMAND ----------

# Define the schema for BPM logs
bpm_schema = StructType([
    StructField("timeMillis", LongType(), True),
    StructField("thread", StringType(), True),
    StructField("level", StringType(), True),
    StructField("loggerName", StringType(), True),
    StructField("message", StructType([
        StructField("ConversationId", StringType(), True),
        StructField("ResponseCode", StringType(), True),
        StructField("ClientApp", StringType(), True),
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
        StructField("Vtier", StringType(), True),
        StructField("TransactionId", StringType(), True),
        StructField("InstanceName", StringType(), True),
        StructField("HostIPAddress", StringType(), True),
        StructField("ServiceKeyData2", StringType(), True),
        StructField("StartTimeStamp", StringType(), True),
        StructField("ServiceKeyData1", StringType(), True),
        StructField("Application", StringType(), True),
        StructField("ReqMsgSize", StringType(), True),
        StructField("RespMsgSize", StringType(), True),
        StructField("HostName", StringType(), True)
    ]), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Streaming DataFrame

# COMMAND ----------

# Read streaming data from the volume
bpm_stream = (spark
    .readStream
    .format("json")
    .schema(bpm_schema)
    .option("maxFilesPerTrigger", config["max_files_per_trigger"])
    .load(config["bpm_logs_path"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Data

# COMMAND ----------

# Add ingestion timestamp and process the data
# Add ingestion timestamp and process the data
bpm_processed = (bpm_stream
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("log_timestamp", from_unixtime(col("timeMillis") / 1000).cast("timestamp"))
    .withColumn("conversation_id", col("message.ConversationId"))
    .withColumn("response_code", col("message.ResponseCode"))
    .withColumn("service", col("message.Service"))
    .withColumn("custom_tag", col("message.CustomTag"))
    .withColumn("cluster", col("message.Cluster"))
    .withColumn("vtier", col("message.Vtier"))
    .withColumn("transaction_id", col("message.TransactionId"))
    .withColumn("instance_name", col("message.InstanceName"))
    .withColumn("host_ip", col("message.HostIPAddress"))
    .withColumn("host_name", col("message.HostName"))
    .withColumn("application", col("message.Application"))
    .withColumn("req_msg_size", col("message.ReqMsgSize").cast("long"))
    .withColumn("resp_msg_size", col("message.RespMsgSize").cast("long"))
    # Extract trail marks information
    .withColumn("trail_marks_count", size(col("message.TrailMarks")))
    # Check if any trail mark has Complete == False using array_contains or filter
    .withColumn("has_incomplete_trails", 
                when(col("trail_marks_count") > 0,
                     exists("message.TrailMarks", lambda x: x.Complete == False))
                .otherwise(False))
    # Calculate total elapsed time from trail marks using transform and aggregate
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
    .withColumn("is_timeout", 
                when((col("response_code") == "408") | 
                     col("has_incomplete_trails"), True).otherwise(False))
    .withColumn("is_failure", 
                when(col("response_code").cast("int") >= 400, True).otherwise(False))
    .withColumn("is_success", 
                when((col("response_code").cast("int") >= 200) & 
                     (col("response_code").cast("int") < 400), True).otherwise(False))
    # High response time flag (> 30 seconds)
    .withColumn("is_high_response_time",
                when(col("total_elapsed_ms") > 30000, True).otherwise(False))
)
# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Table

# COMMAND ----------

# Ensure the database exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config['catalog']}.{config['schema']}")

# COMMAND ----------

# Write the streaming data to Delta table
query = (bpm_processed
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", config["bpm_checkpoint"])
    .trigger(processingTime=config["trigger_processing_time"])
    .option("mergeSchema", "true")
    .toTable(config["bpm_table"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor Stream

# COMMAND ----------

# Display stream status
print(f"Stream ID: {query.id}")
print(f"Stream Status: {query.status}")
print(f"\nStreaming BPM logs from {config['bpm_logs_path']}")
print(f"Writing to table: {config['bpm_table']}")
print(f"Checkpoint location: {config['bpm_checkpoint']}")
print("\nStream is running. Monitor progress in the Spark UI.")

# COMMAND ----------

# Keep the stream running
query.awaitTermination()

