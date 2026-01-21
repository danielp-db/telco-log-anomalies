# Databricks notebook source
"""
MAF Performance Logs Streaming Ingestion
Continuously reads JSON MAF performance logs from Volume and writes to Delta table using Structured Streaming.
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
    "maf_perf_logs_path": f"/Volumes/{catalog}/{schema}/{volume_name}/maf_perf",
    "maf_perf_checkpoint": f"/Volumes/{catalog}/{schema}/{volume_name}/_checkpoints/maf_perf",
    "maf_perf_table": f"{catalog}.{schema}.{yaml_config['log_types']['maf_perf']}",
    "max_files_per_trigger": yaml_config["streaming"]["max_files_per_trigger"],
    "trigger_processing_time": yaml_config["streaming"]["trigger_processing_time"],
}

print("Configuration loaded:")
print(f"  Source: {config['maf_perf_logs_path']}")
print(f"  Target: {config['maf_perf_table']}")
print(f"  Checkpoint: {config['maf_perf_checkpoint']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Schema

# COMMAND ----------

# Define the schema for MAF performance logs (same as M2E performance)
maf_perf_schema = StructType([
    StructField("contextMap", MapType(StringType(), StringType()), True),
    StructField("endOfBatch", BooleanType(), True),
    StructField("filename", StringType(), True),
    StructField("level", StringType(), True),
    StructField("log_uuid", StringType(), True),
    StructField("loggerFqcn", StringType(), True),
    StructField("loggerName", StringType(), True),
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
        StructField("HostName", StringType(), True),
        StructField("ResponseCode", StringType(), True),
        StructField("ExternalFaultCode", StringType(), True),
        StructField("ExternalFaultDescription", StringType(), True),
        StructField("FaultEntity", StringType(), True)
    ]), True),
    StructField("thread", StringType(), True),
    StructField("threadId", LongType(), True),
    StructField("threadPriority", LongType(), True),
    StructField("timeMillis", LongType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Streaming DataFrame

# COMMAND ----------

# Read streaming data from the volume
maf_perf_stream = (spark
    .readStream
    .format("json")
    .schema(maf_perf_schema)
    .option("maxFilesPerTrigger", config["max_files_per_trigger"])
    .load(config["maf_perf_logs_path"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Data

# COMMAND ----------

# Add ingestion timestamp and process the data
maf_perf_processed = (maf_perf_stream
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("log_timestamp", from_unixtime(col("timeMillis") / 1000).cast("timestamp"))
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
    .withColumn("response_code", col("message.ResponseCode"))
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
    .withColumn("is_failure", 
                when(col("response_code").isNotNull() & 
                     col("response_code").cast("int").between(400, 599), True)
                .otherwise(False))
    # High response time flag (> 30 seconds)
    .withColumn("is_high_response_time",
                when(col("total_elapsed_ms") > 30000, True).otherwise(False))
    # Extract context map tracing info
    .withColumn("service_name", col("contextMap")["service.name"])
    .withColumn("span_id", col("contextMap")["span_id"])
    .withColumn("trace_id", col("contextMap")["trace_id"])
    .withColumn("trace_flags", col("contextMap")["trace_flags"])
    # Extract fault information
    .withColumn("external_fault_code", col("message.ExternalFaultCode"))
    .withColumn("external_fault_description", col("message.ExternalFaultDescription"))
    .withColumn("fault_entity", col("message.FaultEntity"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Table

# COMMAND ----------

# Ensure the database exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config['catalog']}.{config['schema']}")

# COMMAND ----------

# Write the streaming data to Delta table
query = (maf_perf_processed
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", config["maf_perf_checkpoint"])
    .trigger(processingTime=config["trigger_processing_time"])
    .option("mergeSchema", "true")
    .toTable(config["maf_perf_table"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor Stream

# COMMAND ----------

# Display stream status
print(f"Stream ID: {query.id}")
print(f"Stream Status: {query.status}")
print(f"\nStreaming MAF performance logs from {config['maf_perf_logs_path']}")
print(f"Writing to table: {config['maf_perf_table']}")
print(f"Checkpoint location: {config['maf_perf_checkpoint']}")
print("\nStream is running. Monitor progress in the Spark UI.")

# COMMAND ----------

# Keep the stream running
query.awaitTermination()

