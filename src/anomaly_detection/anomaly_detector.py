# Databricks notebook source
"""
Anomaly Detection System
Analyzes log data from Delta tables to detect various types of anomalies including:
- Failure rate spikes
- Missing heartbeats
- High response times
- Transaction timeouts
- Log volume anomalies
- Service degradation patterns
"""

# COMMAND ----------

import yaml
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import uuid

# COMMAND ----------

# Load YAML configuration
with open("../../config/common_config.yml", "r") as f:
    yaml_config = yaml.safe_load(f)

# Setup widgets for configuration parameters
dbutils.widgets.text("catalog", yaml_config["database"]["catalog"], "Catalog")
dbutils.widgets.text("schema", yaml_config["database"]["schema"], "Schema")

# Get widget values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# Build configuration dictionary
config = {
    "catalog": catalog,
    "schema": schema,
    "audit_table": f"{catalog}.{schema}.audit_logs",
    "bpm_table": f"{catalog}.{schema}.bpm_logs",
    "performance_table": f"{catalog}.{schema}.performance_logs",
    "anomalies_table": f"{catalog}.{schema}.anomalies",
    **yaml_config["anomaly_detection"],
    **yaml_config["time_windows"],
}

print("Configuration loaded:")
print(f"  Catalog: {config['catalog']}")
print(f"  Schema: {config['schema']}")
print(f"  Anomalies Table: {config['anomalies_table']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_time_windows(current_time=None):
    """
    Get time windows for anomaly detection
    
    Returns:
        Dictionary with time window boundaries
    """
    if current_time is None:
        current_time = datetime.utcnow()
    
    return {
        "current_time": current_time,
        "short_window_start": current_time - timedelta(minutes=config["short_window_minutes"]),
        "medium_window_start": current_time - timedelta(minutes=config["medium_window_minutes"]),
        "long_window_start": current_time - timedelta(minutes=config["long_window_minutes"]),
        "baseline_start": current_time - timedelta(days=config["baseline_window_days"]),
        "baseline_end": current_time - timedelta(hours=1)  # Exclude last hour from baseline
    }

# COMMAND ----------

def create_anomaly_record(anomaly_type, severity, time_window_start, time_window_end,
                         affected_service=None, affected_instance=None, affected_cluster=None,
                         metric_name=None, metric_value=None, threshold_value=None,
                         baseline_value=None, details=None, log_count=None, metadata=None):
    """
    Create a standardized anomaly record
    """
    return {
        "anomaly_id": str(uuid.uuid4()),
        "anomaly_type": anomaly_type,
        "severity": severity,
        "detected_at": datetime.utcnow(),
        "time_window_start": time_window_start,
        "time_window_end": time_window_end,
        "affected_service": affected_service,
        "affected_instance": affected_instance,
        "affected_cluster": affected_cluster,
        "metric_name": metric_name,
        "metric_value": float(metric_value) if metric_value is not None else None,
        "threshold_value": float(threshold_value) if threshold_value is not None else None,
        "baseline_value": float(baseline_value) if baseline_value is not None else None,
        "details": details,
        "log_count": log_count,
        "metadata": metadata or {}
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anomaly Detection Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Failure Rate Spike Detection

# COMMAND ----------

def detect_failure_rate_spikes(config, time_windows):
    """
    Detect sudden spikes in failure rates compared to baseline
    """
    print("\n=== Detecting Failure Rate Spikes ===")
    
    audit_table = config["audit_table"]
    anomalies = []
    
    # Read audit logs
    audit_logs = spark.table(audit_table)
    
    # Calculate current failure rate (last 15 minutes)
    current_failures = (audit_logs
        .filter((col("log_timestamp") >= lit(time_windows["medium_window_start"])) &
                (col("log_timestamp") <= lit(time_windows["current_time"])))
        .groupBy("transaction_name")
        .agg(
            count("*").alias("total_count"),
            sum(when(col("is_failure"), 1).otherwise(0)).alias("failure_count")
        )
        .withColumn("current_failure_rate", 
                    (col("failure_count") / col("total_count")) * 100)
    )
    
    # Calculate baseline failure rate (past 7 days, excluding last hour)
    baseline_failures = (audit_logs
        .filter((col("log_timestamp") >= lit(time_windows["baseline_start"])) &
                (col("log_timestamp") <= lit(time_windows["baseline_end"])))
        .groupBy("transaction_name")
        .agg(
            count("*").alias("baseline_total"),
            sum(when(col("is_failure"), 1).otherwise(0)).alias("baseline_failure_count")
        )
        .withColumn("baseline_failure_rate", 
                    (col("baseline_failure_count") / col("baseline_total")) * 100)
    )
    
    # Compare current vs baseline
    comparison = (current_failures
        .join(baseline_failures, "transaction_name", "left")
        .withColumn("rate_multiplier", 
                    col("current_failure_rate") / (col("baseline_failure_rate") + 0.01))
        .filter(col("rate_multiplier") >= config["failure_rate_threshold_multiplier"])
        .filter(col("total_count") >= 10)  # Minimum sample size
    )
    
    # Collect anomalies
    for row in comparison.collect():
        severity = "CRITICAL" if row.rate_multiplier >= 5 else "HIGH"
        
        anomaly = create_anomaly_record(
            anomaly_type="FAILURE_RATE_SPIKE",
            severity=severity,
            time_window_start=time_windows["medium_window_start"],
            time_window_end=time_windows["current_time"],
            affected_service=row.transaction_name,
            metric_name="failure_rate_percent",
            metric_value=row.current_failure_rate,
            threshold_value=row.baseline_failure_rate * config["failure_rate_threshold_multiplier"],
            baseline_value=row.baseline_failure_rate,
            details=f"Failure rate spike detected: {row.current_failure_rate:.2f}% vs baseline {row.baseline_failure_rate:.2f}%",
            log_count=row.total_count,
            metadata={
                "rate_multiplier": str(row.rate_multiplier),
                "failure_count": str(row.failure_count),
                "baseline_total": str(row.baseline_total)
            }
        )
        anomalies.append(anomaly)
        print(f"  - {row.transaction_name}: {row.current_failure_rate:.2f}% failure rate (baseline: {row.baseline_failure_rate:.2f}%)")
    
    print(f"Found {len(anomalies)} failure rate spike anomalies")
    return anomalies

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Missing Heartbeat Detection

# COMMAND ----------

def detect_missing_heartbeats(config, time_windows):
    """
    Detect services that haven't sent logs in the expected time window
    """
    print("\n=== Detecting Missing Heartbeats ===")
    
    anomalies = []
    threshold_time = time_windows["current_time"] - timedelta(minutes=config["missing_heartbeat_interval_minutes"])
    
    # Check all three log types
    for table_name, table_key in [
        (config["audit_table"], "audit"),
        (config["bpm_table"], "bpm"),
        (config["performance_table"], "performance")
    ]:
        logs = spark.table(table_name)
        
        # For audit_table, add service=None
        if table_name == config["audit_table"]:
            logs = logs.withColumn("service", lit(None).cast(StringType()))
        
        # Get services that were active in the baseline period
        baseline_services = (logs
            .filter((col("log_timestamp") >= lit(time_windows["baseline_start"])) &
                    (col("log_timestamp") <= lit(time_windows["baseline_end"])))
            .select("service", "instance_name", "cluster")
            .distinct()
        ).collect()
        
        # Check which services are missing in the recent window
        recent_logs = (logs
            .filter(col("log_timestamp") >= lit(threshold_time))
            .select("service", "instance_name", "cluster")
            .distinct()
        )
        
        recent_services = set([(r.service, r.instance_name, r.cluster) for r in recent_logs.collect()])
        
        for baseline_svc in baseline_services:
            key = (baseline_svc.service, baseline_svc.instance_name, baseline_svc.cluster)
            if key not in recent_services and baseline_svc.service is not None:
                anomaly = create_anomaly_record(
                    anomaly_type="MISSING_HEARTBEAT",
                    severity="HIGH",
                    time_window_start=threshold_time,
                    time_window_end=time_windows["current_time"],
                    affected_service=baseline_svc.service,
                    affected_instance=baseline_svc.instance_name,
                    affected_cluster=baseline_svc.cluster,
                    metric_name="minutes_since_last_log",
                    metric_value=config["missing_heartbeat_interval_minutes"],
                    threshold_value=config["missing_heartbeat_interval_minutes"],
                    details=f"No logs received from {baseline_svc.service} in the last {config['missing_heartbeat_interval_minutes']} minutes",
                    metadata={"log_type": table_key}
                )
                anomalies.append(anomaly)
                print(f"  - {table_key}: {baseline_svc.service} (instance: {baseline_svc.instance_name})")
    
    print(f"Found {len(anomalies)} missing heartbeat anomalies")
    return anomalies

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. High Response Time Detection

# COMMAND ----------

def detect_high_response_times(config, time_windows):
    """
    Detect transactions with abnormally high response times
    """
    print("\n=== Detecting High Response Times ===")
    
    anomalies = []
    
    # Check audit logs
    audit_logs = (spark.table(config["audit_table"])
        .filter((col("log_timestamp") >= lit(time_windows["long_window_start"])) &
                (col("log_timestamp") <= lit(time_windows["current_time"])))
        .filter(col("response_time_ms").isNotNull())
    )
    
    # Calculate percentiles by service
    audit_stats = (audit_logs
        .groupBy("transaction_name")
        .agg(
            expr(f"percentile(response_time_ms, 0.95)").alias("p95"),
            expr(f"percentile(response_time_ms, 0.99)").alias("p99"),
            avg("response_time_ms").alias("avg_response_time"),
            count("*").alias("count")
        )
        .filter(col("count") >= 10)
    )
    
    # Detect services with high P95 or P99
    high_response = (audit_stats
        .filter((col("p95") > config["high_response_time_p95_threshold_ms"]) |
                (col("p99") > config["high_response_time_p99_threshold_ms"]))
    )
    
    for row in high_response.collect():
        if row.p99 > config["high_response_time_p99_threshold_ms"]:
            severity = "CRITICAL"
            metric = row.p99
            metric_name = "p99_response_time_ms"
            threshold = config["high_response_time_p99_threshold_ms"]
        else:
            severity = "HIGH"
            metric = row.p95
            metric_name = "p95_response_time_ms"
            threshold = config["high_response_time_p95_threshold_ms"]
        
        anomaly = create_anomaly_record(
            anomaly_type="HIGH_RESPONSE_TIME",
            severity=severity,
            time_window_start=time_windows["long_window_start"],
            time_window_end=time_windows["current_time"],
            affected_service=row.transaction_name,
            metric_name=metric_name,
            metric_value=metric,
            threshold_value=threshold,
            baseline_value=row.avg_response_time,
            details=f"High response time detected: {metric_name}={metric:.0f}ms (avg: {row.avg_response_time:.0f}ms)",
            log_count=row.count,
            metadata={
                "p95": str(row.p95),
                "p99": str(row.p99)
            }
        )
        anomalies.append(anomaly)
        print(f"  - {row.transaction_name}: {metric_name}={metric:.0f}ms (threshold: {threshold}ms)")
    
    # Check performance logs
    perf_logs = (spark.table(config["performance_table"])
        .filter((col("log_timestamp") >= lit(time_windows["long_window_start"])) &
                (col("log_timestamp") <= lit(time_windows["current_time"])))
        .filter(col("response_time_ms").isNotNull())
    )
    
    perf_stats = (perf_logs
        .groupBy("service")
        .agg(
            expr(f"percentile(response_time_ms, 0.95)").alias("p95"),
            expr(f"percentile(response_time_ms, 0.99)").alias("p99"),
            avg("response_time_ms").alias("avg_response_time"),
            count("*").alias("count_rows")
        )
        .filter(col("count") >= 10)
        .filter((col("p95") > config["high_response_time_p95_threshold_ms"]) |
                (col("p99") > config["high_response_time_p99_threshold_ms"]))
    )
    
    for row in perf_stats.collect():
        if row.p99 > config["high_response_time_p99_threshold_ms"]:
            severity = "CRITICAL"
            metric = row.p99
            metric_name = "p99_response_time_ms"
            threshold = config["high_response_time_p99_threshold_ms"]
        else:
            severity = "HIGH"
            metric = row.p95
            metric_name = "p95_response_time_ms"
            threshold = config["high_response_time_p95_threshold_ms"]
        
        anomaly = create_anomaly_record(
            anomaly_type="HIGH_RESPONSE_TIME",
            severity=severity,
            time_window_start=time_windows["long_window_start"],
            time_window_end=time_windows["current_time"],
            affected_service=row.service,
            metric_name=metric_name,
            metric_value=metric,
            threshold_value=threshold,
            baseline_value=row.avg_response_time,
            details=f"High response time detected: {metric_name}={metric:.0f}ms (avg: {row.avg_response_time:.0f}ms)",
            log_count=row.count_rows,
            metadata={
                "p95": str(row.p95),
                "p99": str(row.p99),
                "log_type": "performance"
            }
        )
        anomalies.append(anomaly)
        print(f"  - {row.service}: {metric_name}={metric:.0f}ms (threshold: {threshold}ms)")
    
    print(f"Found {len(anomalies)} high response time anomalies")
    return anomalies
    
# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Transaction Timeout Detection

# COMMAND ----------

def detect_transaction_timeouts(config, time_windows):
    """
    Detect incomplete transactions that likely timed out
    """
    print("\n=== Detecting Transaction Timeouts ===")
    
    anomalies = []
    
    # Check BPM logs for incomplete trails
    bpm_logs = spark.table(config["bpm_table"])
    
    timeouts = (bpm_logs
        .filter((col("log_timestamp") >= lit(time_windows["long_window_start"])) &
                (col("log_timestamp") <= lit(time_windows["current_time"])))
        .filter(col("has_incomplete_trails") == True)
        .groupBy("service", "cluster")
        .agg(
            count("*").alias("timeout_count"),
            avg("total_elapsed_ms").alias("avg_elapsed_before_timeout")
        )
        .filter(col("timeout_count") >= 3)
    )
    
    for row in timeouts.collect():
        anomaly = create_anomaly_record(
            anomaly_type="TRANSACTION_TIMEOUT",
            severity="HIGH",
            time_window_start=time_windows["long_window_start"],
            time_window_end=time_windows["current_time"],
            affected_service=row.service,
            affected_cluster=row.cluster,
            metric_name="timeout_count",
            metric_value=row.timeout_count,
            threshold_value=3,
            details=f"Multiple incomplete transactions detected: {row.timeout_count} timeouts",
            log_count=row.timeout_count,
            metadata={
                "avg_elapsed_before_timeout_ms": str(row.avg_elapsed_before_timeout),
                "log_type": "bpm"
            }
        )
        anomalies.append(anomaly)
        print(f"  - {row.service}: {row.timeout_count} timeouts (avg elapsed: {row.avg_elapsed_before_timeout:.0f}ms)")
    
    # Check performance logs for incomplete trails
    perf_logs = spark.table(config["performance_table"])
    
    perf_timeouts = (perf_logs
        .filter((col("log_timestamp") >= lit(time_windows["long_window_start"])) &
                (col("log_timestamp") <= lit(time_windows["current_time"])))
        .filter(col("has_incomplete_trails") == True)
        .groupBy("service", "cluster", "instance_name")
        .agg(
            count("*").alias("timeout_count"),
            avg("incomplete_trail_count").alias("avg_incomplete_trails")
        )
        .filter(col("timeout_count") >= 3)
    )
    
    for row in perf_timeouts.collect():
        anomaly = create_anomaly_record(
            anomaly_type="TRANSACTION_TIMEOUT",
            severity="HIGH",
            time_window_start=time_windows["long_window_start"],
            time_window_end=time_windows["current_time"],
            affected_service=row.service,
            affected_instance=row.instance_name,
            affected_cluster=row.cluster,
            metric_name="timeout_count",
            metric_value=row.timeout_count,
            threshold_value=3,
            details=f"Multiple incomplete transactions detected: {row.timeout_count} timeouts",
            log_count=row.timeout_count,
            metadata={
                "avg_incomplete_trails": str(row.avg_incomplete_trails),
                "log_type": "performance"
            }
        )
        anomalies.append(anomaly)
        print(f"  - {row.service} ({row.instance_name}): {row.timeout_count} timeouts")
    
    print(f"Found {len(anomalies)} transaction timeout anomalies")
    return anomalies

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Log Volume Anomaly Detection

# COMMAND ----------

def detect_volume_anomalies(config, time_windows):
    """
    Detect sudden drops or spikes in log volume
    """
    print("\n=== Detecting Log Volume Anomalies ===")
    
    anomalies = []
    
    # Check each log type
    for table_name, table_key in [
        (config["audit_table"], "audit"),
        (config["bpm_table"], "bpm"),
        (config["performance_table"], "performance")
    ]:
        logs = spark.table(table_name)
        
        # Calculate current volume (last 15 minutes)
        current_volume = (logs
            .filter((col("log_timestamp") >= lit(time_windows["medium_window_start"])) &
                    (col("log_timestamp") <= lit(time_windows["current_time"])))
            .groupBy("service")
            .agg(count("*").alias("current_count"))
        )
        
        # Calculate baseline volume (same 15-minute window, averaged over past 7 days)
        minutes_diff = config["medium_window_minutes"]
        baseline_volume = (logs
            .filter((col("log_timestamp") >= lit(time_windows["baseline_start"])) &
                    (col("log_timestamp") <= lit(time_windows["baseline_end"])))
            .withColumn("time_bucket", 
                        floor(unix_timestamp("log_timestamp") / (minutes_diff * 60)))
            .groupBy("service", "time_bucket")
            .agg(count("*").alias("bucket_count"))
            .groupBy("service")
            .agg(avg("bucket_count").alias("baseline_avg_count"))
        )
        
        # Compare volumes
        comparison = (current_volume
            .join(baseline_volume, "service", "left")
            .filter(col("baseline_avg_count").isNotNull())
            .withColumn("volume_ratio", 
                        col("current_count") / (col("baseline_avg_count") + 0.1))
            .filter((col("volume_ratio") > config["volume_anomaly_threshold_multiplier"]) |
                    (col("volume_ratio") < (1.0 / config["volume_anomaly_threshold_multiplier"])))
        )
        
        for row in comparison.collect():
            if row.volume_ratio > config["volume_anomaly_threshold_multiplier"]:
                anomaly_subtype = "VOLUME_SPIKE"
                severity = "MEDIUM"
                details = f"Log volume spike: {row.current_count} logs vs baseline {row.baseline_avg_count:.0f}"
            else:
                anomaly_subtype = "VOLUME_DROP"
                severity = "HIGH"
                details = f"Log volume drop: {row.current_count} logs vs baseline {row.baseline_avg_count:.0f}"
            
            anomaly = create_anomaly_record(
                anomaly_type=anomaly_subtype,
                severity=severity,
                time_window_start=time_windows["medium_window_start"],
                time_window_end=time_windows["current_time"],
                affected_service=row.service,
                metric_name="log_count",
                metric_value=row.current_count,
                threshold_value=row.baseline_avg_count * config["volume_anomaly_threshold_multiplier"],
                baseline_value=row.baseline_avg_count,
                details=details,
                log_count=row.current_count,
                metadata={
                    "volume_ratio": str(row.volume_ratio),
                    "log_type": table_key
                }
            )
            anomalies.append(anomaly)
            print(f"  - {table_key}/{row.service}: {anomaly_subtype} (ratio: {row.volume_ratio:.2f}x)")
    
    print(f"Found {len(anomalies)} log volume anomalies")
    return anomalies

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Service Degradation Detection

# COMMAND ----------

def detect_service_degradation(config, time_windows):
    """
    Detect gradual performance degradation trends
    """
    print("\n=== Detecting Service Degradation ===")
    
    anomalies = []
    
    # Check performance logs
    perf_logs = spark.table(config["performance_table"])
    
    # Calculate response times for recent period vs baseline
    recent_perf = (perf_logs
        .filter((col("log_timestamp") >= lit(time_windows["medium_window_start"])) &
                (col("log_timestamp") <= lit(time_windows["current_time"])))
        .filter(col("response_time_ms").isNotNull())
        .groupBy("service", "instance_name")
        .agg(
            avg("response_time_ms").alias("recent_avg_response_time"),
            count("*").alias("recent_count")
        )
        .filter(col("recent_count") >= 5)
    )
    
    baseline_perf = (perf_logs
        .filter((col("log_timestamp") >= lit(time_windows["baseline_start"])) &
                (col("log_timestamp") <= lit(time_windows["baseline_end"])))
        .filter(col("response_time_ms").isNotNull())
        .groupBy("service", "instance_name")
        .agg(
            avg("response_time_ms").alias("baseline_avg_response_time"),
            count("*").alias("baseline_count")
        )
        .filter(col("baseline_count") >= 10)
    )
    
    # Compare recent vs baseline
    degradation = (recent_perf
        .join(baseline_perf, ["service", "instance_name"], "inner")
        .withColumn("degradation_percent",
                    ((col("recent_avg_response_time") - col("baseline_avg_response_time")) / 
                     col("baseline_avg_response_time")) * 100)
        .filter(col("degradation_percent") >= config["degradation_increase_threshold_percent"])
    )
    
    for row in degradation.collect():
        severity = "CRITICAL" if row.degradation_percent >= 100 else "HIGH"
        
        anomaly = create_anomaly_record(
            anomaly_type="SERVICE_DEGRADATION",
            severity=severity,
            time_window_start=time_windows["medium_window_start"],
            time_window_end=time_windows["current_time"],
            affected_service=row.service,
            affected_instance=row.instance_name,
            metric_name="avg_response_time_ms",
            metric_value=row.recent_avg_response_time,
            threshold_value=row.baseline_avg_response_time * (1 + config["degradation_increase_threshold_percent"] / 100),
            baseline_value=row.baseline_avg_response_time,
            details=f"Performance degradation: {row.degradation_percent:.1f}% slower than baseline ({row.recent_avg_response_time:.0f}ms vs {row.baseline_avg_response_time:.0f}ms)",
            log_count=row.recent_count,
            metadata={
                "degradation_percent": str(row.degradation_percent),
                "baseline_count": str(row.baseline_count)
            }
        )
        anomalies.append(anomaly)
        print(f"  - {row.service} ({row.instance_name}): {row.degradation_percent:.1f}% slower")
    
    print(f"Found {len(anomalies)} service degradation anomalies")
    return anomalies

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Detection Orchestration

# COMMAND ----------

def run_anomaly_detection(config):
    """
    Run all anomaly detection algorithms and return combined results
    """
    print("\n" + "="*80)
    print("STARTING ANOMALY DETECTION RUN")
    print(f"Run Time: {datetime.utcnow().isoformat()}")
    print("="*80)
    
    # Get time windows
    time_windows = get_time_windows()
    
    # Run all detection algorithms
    all_anomalies = []
    
    try:
        all_anomalies.extend(detect_failure_rate_spikes(config, time_windows))
    except Exception as e:
        print(f"Error in failure rate spike detection: {str(e)}")
    
    try:
        all_anomalies.extend(detect_missing_heartbeats(config, time_windows))
    except Exception as e:
        print(f"Error in missing heartbeat detection: {str(e)}")
    
    try:
        all_anomalies.extend(detect_high_response_times(config, time_windows))
    except Exception as e:
        print(f"Error in high response time detection: {str(e)}")
    
    try:
        all_anomalies.extend(detect_transaction_timeouts(config, time_windows))
    except Exception as e:
        print(f"Error in transaction timeout detection: {str(e)}")
    
    try:
        all_anomalies.extend(detect_volume_anomalies(config, time_windows))
    except Exception as e:
        print(f"Error in volume anomaly detection: {str(e)}")
    
    try:
        all_anomalies.extend(detect_service_degradation(config, time_windows))
    except Exception as e:
        print(f"Error in service degradation detection: {str(e)}")
    
    print("\n" + "="*80)
    print(f"DETECTION COMPLETE: Found {len(all_anomalies)} total anomalies")
    print("="*80)
    
    return all_anomalies

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Anomalies to Table

# COMMAND ----------

def save_anomalies(anomalies, config):
    """
    Save detected anomalies to Delta table
    """
    if not anomalies:
        print("No anomalies to save.")
        return
    
    # Convert to DataFrame
    anomalies_df = spark.createDataFrame(anomalies)
    
    # Ensure the database exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config['catalog']}.{config['schema']}")
    
    # Write to Delta table
    (anomalies_df
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(config["anomalies_table"])
    )
    
    print(f"\nSaved {len(anomalies)} anomalies to {config['anomalies_table']}")
    
    # Display summary
    summary = (anomalies_df
        .groupBy("anomaly_type", "severity")
        .agg(count("*").alias("count"))
        .orderBy("severity", "anomaly_type")
    )
    
    print("\nAnomaly Summary:")
    summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Detection

# COMMAND ----------

# Run the anomaly detection
anomalies = run_anomaly_detection(config)

# Save results
save_anomalies(anomalies, config)

print("\nAnomaly detection completed successfully!")

