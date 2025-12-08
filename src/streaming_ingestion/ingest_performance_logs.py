# Databricks notebook source
"""
Performance Tracking Logs Streaming Ingestion
Continuously reads JSON performance tracking logs from Volume and writes to Delta table using Structured Streaming.
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
    "performance_logs_path": f"/Volumes/{catalog}/{schema}/{volume_name}/performance",
    "performance_checkpoint": f"/Volumes/{catalog}/{schema}/{volume_name}/_checkpoints/performance",
    "performance_table": f"{catalog}.{schema}.performance_logs",
    "max_files_per_trigger": yaml_config["streaming"]["max_files_per_trigger"],
    "trigger_processing_time": yaml_config["streaming"]["trigger_processing_time"],
}

print("Configuration loaded:")
print(f"  Source: {config['performance_logs_path']}")
print(f"  Target: {config['performance_table']}")
print(f"  Checkpoint: {config['performance_checkpoint']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Schema

# COMMAND ----------

# Define the schema for performance tracking logs
performance_schema = StructType([
    StructField("timeMillis", LongType(), True),
    StructField("thread", StringType(), True),
    StructField("level", StringType(), True),
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
        StructField("ResponseCode", StringType(), True)
    ]), True),
    StructField("endOfBatch", BooleanType(), True),
    StructField("loggerFqcn", StringType(), True),
    StructField("contextMap", MapType(StringType(), StringType()), True),
    StructField("threadId", LongType(), True),
    StructField("threadPriority", LongType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Streaming DataFrame

# COMMAND ----------

# Read streaming data from the volume
performance_stream = (spark
    .readStream
    .format("json")
    .schema(performance_schema)
    .option("maxFilesPerTrigger", config["max_files_per_trigger"])
    .load(config["performance_logs_path"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Data

# COMMAND ----------

# Add ingestion timestamp and process the data
# Add ingestion timestamp and process the data
performance_processed = (performance_stream
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("log_timestamp", from_unixtime(col("timeMillis") / 1000).cast("timestamp"))
    .withColumn("conversation_id", col("message.ConversationId"))
    .withColumn("client_app", col("message.ClientApp"))
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
    # Parse start timestamp
    .withColumn("start_timestamp",
                when(col("message.StartTimeStamp").isNotNull(),
                     to_timestamp(col("message.StartTimeStamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'"))
                .otherwise(None))
    # Extract trail marks information
    .withColumn("trail_marks_count", size(col("message.TrailMarks")))
    .withColumn("has_incomplete_trails", 
                exists(col("message.TrailMarks"), lambda x: x.Complete == False)) 
    .withColumn("incomplete_trail_count",
                aggregate(
                    col("message.TrailMarks"),
                    lit(0),
                    lambda acc, x: acc + when(x.Complete == False, lit(1)).otherwise(lit(0))
                ))
    # Calculate total elapsed time from trail marks
    .withColumn("total_elapsed_ms",
                when(col("trail_marks_count") > 0,
                     aggregate(
                         col("message.TrailMarks"),
                         lit(0).cast("bigint"),
                         lambda acc, x: acc + coalesce(x.ElapsedDuration, lit(0)).cast("bigint")
                     )
                ).otherwise(None))
    # Calculate min and max elapsed duration from trail marks
    .withColumn("min_trail_elapsed_ms",
                when(col("trail_marks_count") > 0,
                     aggregate(
                         col("message.TrailMarks"),
                         lit(999999999).cast("bigint"),
                         lambda acc, x: when(x.ElapsedDuration.isNotNull() & (x.ElapsedDuration < acc), 
                                             x.ElapsedDuration).otherwise(acc).cast("bigint")
                     )
                ).otherwise(None))
    .withColumn("max_trail_elapsed_ms",
                when(col("trail_marks_count") > 0,
                     aggregate(
                         col("message.TrailMarks"),
                         lit(0).cast("bigint"),
                         lambda acc, x: when(x.ElapsedDuration.isNotNull() & (x.ElapsedDuration > acc), 
                                             x.ElapsedDuration).otherwise(acc).cast("bigint")
                     )
                ).otherwise(None))
    # Get first and last trail mark timestamps
    .withColumn("first_trail_start",
                when(col("trail_marks_count") > 0,
                     to_timestamp(element_at(col("message.TrailMarks"), 1).StartTime, 
                                  "yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'"))
                .otherwise(None))
    .withColumn("last_trail_end",
                when(col("trail_marks_count") > 0,
                     to_timestamp(element_at(col("message.TrailMarks"), -1).EndTime, 
                                  "yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'"))
                .otherwise(None))
    # Calculate actual response time from timestamps if available
    .withColumn("response_time_calculated_ms",
                when(col("last_trail_end").isNotNull() & col("first_trail_start").isNotNull(),
                     (unix_timestamp(col("last_trail_end")) - unix_timestamp(col("first_trail_start"))) * 1000)
                .otherwise(None))
    .withColumn("response_time_ms",
                coalesce(col("total_elapsed_ms"), col("response_time_calculated_ms")))
    # Classify log
    .withColumn("is_error", when(col("level").isin("ERROR", "FATAL"), True).otherwise(False))
    .withColumn("is_timeout", col("has_incomplete_trails"))
    .withColumn("is_failure", 
                when(col("response_code").isNotNull() & 
                     (col("response_code").cast("int") >= 400), True).otherwise(False))
    .withColumn("is_success", 
                when(col("response_code").isNotNull() & 
                     (col("response_code").cast("int") >= 200) & 
                     (col("response_code").cast("int") < 400), True).otherwise(False))
    # Performance thresholds
    .withColumn("is_high_response_time",
                when(col("response_time_ms") > 30000, True).otherwise(False))
    .withColumn("is_very_high_response_time",
                when(col("response_time_ms") > 60000, True).otherwise(False))
    # Extract span and trace IDs from context map
    .withColumn("span_id", col("contextMap.span_id"))
    .withColumn("trace_id", col("contextMap.trace_id"))
    .withColumn("trace_flags", col("contextMap.trace_flags"))
    .withColumn("service_name", col("contextMap")["service.name"])
)
# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Table

# COMMAND ----------

# Ensure the database exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config['catalog']}.{config['schema']}")

# COMMAND ----------

# Write the streaming data to Delta table
query = (performance_processed
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", config["performance_checkpoint"])
    .trigger(processingTime=config["trigger_processing_time"])
    .option("mergeSchema", "true")
    .toTable(config["performance_table"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor Stream

# COMMAND ----------

# Display stream status
print(f"Stream ID: {query.id}")
print(f"Stream Status: {query.status}")
print(f"\nStreaming performance logs from {config['performance_logs_path']}")
print(f"Writing to table: {config['performance_table']}")
print(f"Checkpoint location: {config['performance_checkpoint']}")
print("\nStream is running. Monitor progress in the Spark UI.")

# COMMAND ----------

# Keep the stream running
query.awaitTermination()

