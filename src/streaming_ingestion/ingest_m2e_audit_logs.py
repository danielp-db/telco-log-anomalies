# Databricks notebook source
"""
M2E Audit Logs Streaming Ingestion
Continuously reads JSON M2E audit logs from Volume and writes to Delta table using Structured Streaming.
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
    "m2e_audit_logs_path": f"/Volumes/{catalog}/{schema}/{volume_name}/m2e_audit",
    "m2e_audit_checkpoint": f"/Volumes/{catalog}/{schema}/{volume_name}/_checkpoints/m2e_audit",
    "m2e_audit_table": f"{catalog}.{schema}.{yaml_config['log_types']['m2e_audit']}",
    "max_files_per_trigger": yaml_config["streaming"]["max_files_per_trigger"],
    "trigger_processing_time": yaml_config["streaming"]["trigger_processing_time"],
}

print("Configuration loaded:")
print(f"  Source: {config['m2e_audit_logs_path']}")
print(f"  Target: {config['m2e_audit_table']}")
print(f"  Checkpoint: {config['m2e_audit_checkpoint']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Schema

# COMMAND ----------

# Define the schema for M2E audit logs (same as BPM audit)
m2e_audit_schema = StructType([
    StructField("contextMap", MapType(StringType(), StringType()), True),
    StructField("endOfBatch", BooleanType(), True),
    StructField("filename", StringType(), True),
    StructField("level", StringType(), True),
    StructField("log_uuid", StringType(), True),
    StructField("loggerFqcn", StringType(), True),
    StructField("loggerName", StringType(), True),
    StructField("message", StructType([
        StructField("Description", StringType(), True),
        StructField("InstanceName", StringType(), True),
        StructField("ApplicationId", StringType(), True),
        StructField("OriginalMessageId", StringType(), True),
        StructField("UniqueTransactionId", StringType(), True),
        StructField("OriginatorId", StringType(), True),
        StructField("Subject", StringType(), True),
        StructField("ConversationId", StringType(), True),
        StructField("OriginationSystemId", StringType(), True),
        StructField("OriginationSystemVersion", StringType(), True),
        StructField("OriginationSystemName", StringType(), True),
        StructField("SourceClass", StringType(), True),
        StructField("SourceMethod", StringType(), True),
        StructField("TransactionName", StringType(), True),
        StructField("TransactionStatus", StringType(), True),
        StructField("HostIPAddress", StringType(), True),
        StructField("HostName", StringType(), True),
        StructField("ResponseCode", StringType(), True),
        StructField("ResponseDescription", StringType(), True),
        StructField("FaultTimestamp", StringType(), True),
        StructField("FaultSequenceNumber", StringType(), True),
        StructField("FaultLevel", StringType(), True),
        StructField("FaultCode", StringType(), True),
        StructField("FaultDescription", StringType(), True),
        StructField("ExternalFaultCode", StringType(), True),
        StructField("ExternalFaultDescription", StringType(), True),
        StructField("FaultEntity", StringType(), True),
        StructField("InitiatedTimestamp", StringType(), True),
        StructField("ElapsedTime", StringType(), True),
        StructField("Mode", StringType(), True),
        StructField("ServiceKeyData1", StringType(), True),
        StructField("ServiceKeyData2", StringType(), True),
        StructField("Cluster", StringType(), True),
        StructField("ClientApp", StringType(), True),
        StructField("Vtier", StringType(), True),
        StructField("ClientIP", StringType(), True),
        StructField("HttpMethod", StringType(), True),
        StructField("RequestURL", StringType(), True)
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
m2e_audit_stream = (spark
    .readStream
    .format("json")
    .schema(m2e_audit_schema)
    .option("maxFilesPerTrigger", config["max_files_per_trigger"])
    .load(config["m2e_audit_logs_path"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Data

# COMMAND ----------

# Add ingestion timestamp and process the data
m2e_audit_processed = (m2e_audit_stream
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("log_timestamp", from_unixtime(col("timeMillis") / 1000).cast("timestamp"))
    .withColumn("transaction_name", col("message.TransactionName"))
    .withColumn("transaction_status", col("message.TransactionStatus"))
    .withColumn("response_code", col("message.ResponseCode"))
    .withColumn("elapsed_time_ms", col("message.ElapsedTime").cast("long"))
    .withColumn("host_name", col("message.HostName"))
    .withColumn("host_ip", col("message.HostIPAddress"))
    .withColumn("cluster", col("message.Cluster"))
    .withColumn("instance_name", col("message.InstanceName"))
    .withColumn("conversation_id", col("message.ConversationId"))
    .withColumn("unique_transaction_id", col("message.UniqueTransactionId"))
    .withColumn("application_id", col("message.ApplicationId"))
    .withColumn("origination_system_name", col("message.OriginationSystemName"))
    # Parse timestamps
    .withColumn("initiated_timestamp", 
                when(col("message.InitiatedTimestamp").isNotNull(),
                     to_timestamp(col("message.InitiatedTimestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'"))
                .otherwise(None))
    .withColumn("fault_timestamp", 
                when(col("message.FaultTimestamp").isNotNull(),
                     to_timestamp(col("message.FaultTimestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'"))
                .otherwise(None))
    # Classify log level
    .withColumn("is_error", when(col("level").isin("ERROR", "FATAL"), True).otherwise(False))
    .withColumn("is_failure", when(col("transaction_status").isin("FAILED", "ERROR", "TIMEOUT"), True).otherwise(False))
    .withColumn("is_success", when(col("transaction_status") == "COMPLETE", True).otherwise(False))
    # Calculate response time from timestamps if elapsed_time is not available
    .withColumn("response_time_calculated_ms",
                when(col("fault_timestamp").isNotNull() & col("initiated_timestamp").isNotNull(),
                     (unix_timestamp(col("fault_timestamp")) - unix_timestamp(col("initiated_timestamp"))) * 1000)
                .otherwise(None))
    .withColumn("response_time_ms",
                coalesce(col("elapsed_time_ms"), col("response_time_calculated_ms")))
    # Extract context map tracing info if available
    .withColumn("service_name", col("contextMap")["service.name"])
    .withColumn("span_id", col("contextMap")["span_id"])
    .withColumn("trace_id", col("contextMap")["trace_id"])
    .withColumn("trace_flags", col("contextMap")["trace_flags"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Table

# COMMAND ----------

# Ensure the database exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config['catalog']}.{config['schema']}")

# COMMAND ----------

# Write the streaming data to Delta table
query = (m2e_audit_processed
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", config["m2e_audit_checkpoint"])
    .trigger(processingTime=config["trigger_processing_time"])
    .option("mergeSchema", "true")
    .toTable(config["m2e_audit_table"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor Stream

# COMMAND ----------

# Display stream status
print(f"Stream ID: {query.id}")
print(f"Stream Status: {query.status}")
print(f"\nStreaming M2E audit logs from {config['m2e_audit_logs_path']}")
print(f"Writing to table: {config['m2e_audit_table']}")
print(f"Checkpoint location: {config['m2e_audit_checkpoint']}")
print("\nStream is running. Monitor progress in the Spark UI.")

# COMMAND ----------

# Keep the stream running
query.awaitTermination()

