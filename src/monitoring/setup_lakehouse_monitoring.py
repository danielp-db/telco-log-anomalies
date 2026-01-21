# Databricks notebook source
"""
Lakehouse Monitoring Setup
Creates and configures Databricks Lakehouse Monitoring monitors for log tables with custom metrics.
Monitors data quality, freshness, and anomaly patterns.
"""

# COMMAND ----------

import yaml
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, LongType
from datetime import datetime

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

print(f"Setting up Lakehouse Monitoring for:")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Lakehouse Monitoring

# COMMAND ----------

import databricks.lakehouse_monitoring as lm

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Change Data Feed on Tables

# COMMAND ----------

# CDF is required for Lakehouse Monitoring to work properly
print("Checking and enabling Change Data Feed (CDF) on tables...\n")

tables_to_monitor = [
    f"{catalog}.{schema}.{yaml_config['log_types']['bpm_adapter']}",
    f"{catalog}.{schema}.{yaml_config['log_types']['bpm_audit']}",
    f"{catalog}.{schema}.{yaml_config['log_types']['bpm_perf']}",
    f"{catalog}.{schema}.{yaml_config['log_types']['m2e_audit']}",
    f"{catalog}.{schema}.{yaml_config['log_types']['m2e_perf']}",
    f"{catalog}.{schema}.{yaml_config['log_types']['maf_perf']}",
    f"{catalog}.{schema}.anomalies"
]

for table_name in tables_to_monitor:
    try:
        # Check if table exists
        table_exists = spark.catalog.tableExists(table_name)
        
        if not table_exists:
            print(f"⚠ Table {table_name} does not exist yet. Will be created by streaming jobs.")
            continue
        
        # Check if CDF is already enabled
        table_properties = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
        cdf_enabled = False
        
        for prop in table_properties:
            if prop.key == "delta.enableChangeDataFeed" and prop.value == "true":
                cdf_enabled = True
                break
        
        if cdf_enabled:
            print(f"✓ {table_name}: CDF already enabled")
        else:
            print(f"  {table_name}: Enabling CDF...")
            spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
            print(f"✓ {table_name}: CDF enabled")
    
    except Exception as e:
        print(f"⚠ {table_name}: Error - {str(e)}")

print("\n✓ CDF check complete\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Custom Metrics

# COMMAND ----------

# Custom metrics for audit logs
audit_custom_metrics = [
    # Failure rate in the last hour
    lm.Metric(
        type="aggregate",
        name="hourly_failure_rate",
        input_columns=["is_failure"],
        definition="avg(cast(is_failure as double))",
        output_data_type=DoubleType()
    ),
    # Average response time
    lm.Metric(
        type="aggregate",
        name="avg_response_time_ms",
        input_columns=["response_time_ms"],
        definition="avg(response_time_ms)",
        output_data_type=DoubleType()
    ),
    # P95 response time
    lm.Metric(
        type="aggregate",
        name="p95_response_time_ms",
        input_columns=["response_time_ms"],
        definition="percentile(response_time_ms, 0.95)",
        output_data_type=DoubleType()
    ),
    # Count of high response time transactions
    lm.Metric(
        type="aggregate",
        name="high_response_time_count",
        input_columns=["response_time_ms"],
        definition="sum(case when response_time_ms > 30000 then 1 else 0 end)",
        output_data_type=LongType()
    ),
    # Unique services count
    lm.Metric(
        type="aggregate",
        name="unique_services",
        input_columns=["transaction_name"],
        definition="count(distinct transaction_name)",
        output_data_type=LongType()
    ),
    # Error rate
    lm.Metric(
        type="aggregate",
        name="error_rate",
        input_columns=["is_error"],
        definition="avg(cast(is_error as double))",
        output_data_type=DoubleType()
    )
]

# Custom metrics for BPM logs
bpm_custom_metrics = [
    # Timeout rate
    lm.Metric(
        type="aggregate",
        name="timeout_rate",
        input_columns=["is_timeout"],
        definition="avg(cast(is_timeout as double))",
        output_data_type=DoubleType()
    ),
    # Average elapsed time
    lm.Metric(
        type="aggregate",
        name="avg_total_elapsed_ms",
        input_columns=["total_elapsed_ms"],
        definition="avg(total_elapsed_ms)",
        output_data_type=DoubleType()
    ),
    # Incomplete trails percentage
    lm.Metric(
        type="aggregate",
        name="incomplete_trails_rate",
        input_columns=["has_incomplete_trails"],
        definition="avg(cast(has_incomplete_trails as double))",
        output_data_type=DoubleType()
    ),
    # High response time rate
    lm.Metric(
        type="aggregate",
        name="high_response_rate",
        input_columns=["is_high_response_time"],
        definition="avg(cast(is_high_response_time as double))",
        output_data_type=DoubleType()
    )
]

# Custom metrics for performance logs
performance_custom_metrics = [
    # Average response time
    lm.Metric(
        type="aggregate",
        name="avg_response_time_ms",
        input_columns=["response_time_ms"],
        definition="avg(response_time_ms)",
        output_data_type=DoubleType()
    ),
    # Max response time
    lm.Metric(
        type="aggregate",
        name="max_response_time_ms",
        input_columns=["response_time_ms"],
        definition="max(response_time_ms)",
        output_data_type=DoubleType()
    ),
    # Incomplete operations rate
    lm.Metric(
        type="aggregate",
        name="incomplete_operations_rate",
        input_columns=["has_incomplete_trails"],
        definition="avg(cast(has_incomplete_trails as double))",
        output_data_type=DoubleType()
    ),
    # Very high response time rate (>60s)
    lm.Metric(
        type="aggregate",
        name="very_high_response_rate",
        input_columns=["is_very_high_response_time"],
        definition="avg(cast(is_very_high_response_time as double))",
        output_data_type=DoubleType()
    ),
    # Average trails per transaction
    lm.Metric(
        type="aggregate",
        name="avg_trail_marks_count",
        input_columns=["trail_marks_count"],
        definition="avg(trail_marks_count)",
        output_data_type=DoubleType()
    )
]

# Custom metrics for anomalies table
anomalies_custom_metrics = [
    # Critical anomalies rate
    lm.Metric(
        type="aggregate",
        name="critical_anomaly_rate",
        input_columns=["severity"],
        definition="avg(case when severity = 'CRITICAL' then 1.0 else 0.0 end)",
        output_data_type=DoubleType()
    ),
    # High severity anomalies rate
    lm.Metric(
        type="aggregate",
        name="high_anomaly_rate",
        input_columns=["severity"],
        definition="avg(case when severity = 'HIGH' then 1.0 else 0.0 end)",
        output_data_type=DoubleType()
    ),
    # Anomalies by type distribution
    lm.Metric(
        type="aggregate",
        name="unique_anomaly_types",
        input_columns=["anomaly_type"],
        definition="count(distinct anomaly_type)",
        output_data_type=LongType()
    ),
    # Average metric value
    lm.Metric(
        type="aggregate",
        name="avg_metric_value",
        input_columns=["metric_value"],
        definition="avg(metric_value)",
        output_data_type=DoubleType()
    ),
    # Threshold breach ratio
    lm.Metric(
        type="aggregate",
        name="avg_threshold_breach_ratio",
        input_columns=["metric_value", "threshold_value"],
        definition="avg(metric_value / nullif(threshold_value, 0))",
        output_data_type=DoubleType()
    )
]

print("✓ Custom metrics defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Monitor for Audit Logs

# COMMAND ----------

audit_table_name = f"{catalog}.{schema}.audit_logs"

try:
    print(f"Creating monitor for {audit_table_name}...")
    
    audit_monitor = lm.create_monitor(
        table_name=audit_table_name,
        profile_type=lm.TimeSeries(
            timestamp_col="log_timestamp",
            granularities=["1 hour", "1 day"]
        ),
        output_schema_name=f"{catalog}.{schema}",
        schedule=lm.MonitorCronSchedule(
            quartz_cron_expression="0 0 */6 * * ?",  # Every 6 hours
            timezone_id="UTC"
        ),
        custom_metrics=audit_custom_metrics,
        baseline_table_name=None,  # No baseline comparison initially
        slicing_exprs=["transaction_name", "cluster"]  # Slice by service and cluster
    )
    
    print(f"✓ Monitor created for audit logs")
    print(f"  Monitor status: {audit_monitor.status}")
    print(f"  Profile metrics table: {audit_monitor.profile_metrics_table_name}")
    
except Exception as e:
    print(f"Error creating audit logs monitor: {str(e)}")
    print("Monitor may already exist. Updating instead...")
    try:
        audit_monitor = lm.update_monitor(
            table_name=audit_table_name,
            updated_params={
                "custom_metrics": audit_custom_metrics,
                "slicing_exprs": ["transaction_name", "cluster"]
            }
        )
        print(f"✓ Monitor updated for audit logs")
    except Exception as e2:
        print(f"Error updating monitor: {str(e2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Monitor for BPM Logs

# COMMAND ----------

bpm_table_name = f"{catalog}.{schema}.bpm_logs"

try:
    print(f"Creating monitor for {bpm_table_name}...")
    
    bpm_monitor = lm.create_monitor(
        table_name=bpm_table_name,
        profile_type=lm.TimeSeries(
            timestamp_col="log_timestamp",
            granularities=["1 hour", "1 day"]
        ),
        output_schema_name=f"{catalog}.{schema}",
        schedule=lm.MonitorCronSchedule(
            quartz_cron_expression="0 0 */6 * * ?",  # Every 6 hours
            timezone_id="UTC"
        ),
        custom_metrics=bpm_custom_metrics,
        baseline_table_name=None,
        slicing_exprs=["service", "cluster"]
    )
    
    print(f"✓ Monitor created for BPM logs")
    print(f"  Monitor status: {bpm_monitor.status}")
    print(f"  Profile metrics table: {bpm_monitor.profile_metrics_table_name}")
    
except Exception as e:
    print(f"Error creating BPM logs monitor: {str(e)}")
    print("Monitor may already exist. Updating instead...")
    try:
        bpm_monitor = lm.update_monitor(
            table_name=bpm_table_name,
            updated_params={
                "custom_metrics": bpm_custom_metrics,
                "slicing_exprs": ["service", "cluster"]
            }
        )
        print(f"✓ Monitor updated for BPM logs")
    except Exception as e2:
        print(f"Error updating monitor: {str(e2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Monitor for Performance Logs

# COMMAND ----------

performance_table_name = f"{catalog}.{schema}.performance_logs"

try:
    print(f"Creating monitor for {performance_table_name}...")
    
    performance_monitor = lm.create_monitor(
        table_name=performance_table_name,
        profile_type=lm.TimeSeries(
            timestamp_col="log_timestamp",
            granularities=["1 hour", "1 day"]
        ),
        output_schema_name=f"{catalog}.{schema}",
        schedule=lm.MonitorCronSchedule(
            quartz_cron_expression="0 0 */6 * * ?",  # Every 6 hours
            timezone_id="UTC"
        ),
        custom_metrics=performance_custom_metrics,
        baseline_table_name=None,
        slicing_exprs=["service", "cluster", "application"]
    )
    
    print(f"✓ Monitor created for performance logs")
    print(f"  Monitor status: {performance_monitor.status}")
    print(f"  Profile metrics table: {performance_monitor.profile_metrics_table_name}")
    
except Exception as e:
    print(f"Error creating performance logs monitor: {str(e)}")
    print("Monitor may already exist. Updating instead...")
    try:
        performance_monitor = lm.update_monitor(
            table_name=performance_table_name,
            updated_params={
                "custom_metrics": performance_custom_metrics,
                "slicing_exprs": ["service", "cluster", "application"]
            }
        )
        print(f"✓ Monitor updated for performance logs")
    except Exception as e2:
        print(f"Error updating monitor: {str(e2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Monitor for Anomalies Table

# COMMAND ----------

anomalies_table_name = f"{catalog}.{schema}.anomalies"

try:
    print(f"Creating monitor for {anomalies_table_name}...")
    
    anomalies_monitor = lm.create_monitor(
        table_name=anomalies_table_name,
        profile_type=lm.TimeSeries(
            timestamp_col="detected_at",
            granularities=["1 hour", "1 day"]
        ),
        output_schema_name=f"{catalog}.{schema}",
        schedule=lm.MonitorCronSchedule(
            quartz_cron_expression="0 0 * * * ?",  # Every hour
            timezone_id="UTC"
        ),
        custom_metrics=anomalies_custom_metrics,
        baseline_table_name=None,
        slicing_exprs=["anomaly_type", "severity", "affected_service"]
    )
    
    print(f"✓ Monitor created for anomalies")
    print(f"  Monitor status: {anomalies_monitor.status}")
    print(f"  Profile metrics table: {anomalies_monitor.profile_metrics_table_name}")
    
except Exception as e:
    print(f"Error creating anomalies monitor: {str(e)}")
    print("Monitor may already exist. Updating instead...")
    try:
        anomalies_monitor = lm.update_monitor(
            table_name=anomalies_table_name,
            updated_params={
                "custom_metrics": anomalies_custom_metrics,
                "slicing_exprs": ["anomaly_type", "severity", "affected_service"]
            }
        )
        print(f"✓ Monitor updated for anomalies")
    except Exception as e2:
        print(f"Error updating monitor: {str(e2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Initial Refresh for All Monitors

# COMMAND ----------

print("Running initial refresh for all monitors...")
print("This may take several minutes depending on data volume.\n")

# Refresh audit logs monitor
try:
    print(f"Refreshing monitor for {audit_table_name}...")
    lm.run_refresh(table_name=audit_table_name)
    print("✓ Audit logs monitor refreshed")
except Exception as e:
    print(f"Error refreshing audit monitor: {str(e)}")

# Refresh BPM logs monitor
try:
    print(f"Refreshing monitor for {bpm_table_name}...")
    lm.run_refresh(table_name=bpm_table_name)
    print("✓ BPM logs monitor refreshed")
except Exception as e:
    print(f"Error refreshing BPM monitor: {str(e)}")

# Refresh performance logs monitor
try:
    print(f"Refreshing monitor for {performance_table_name}...")
    lm.run_refresh(table_name=performance_table_name)
    print("✓ Performance logs monitor refreshed")
except Exception as e:
    print(f"Error refreshing performance monitor: {str(e)}")

# Refresh anomalies monitor
try:
    print(f"Refreshing monitor for {anomalies_table_name}...")
    lm.run_refresh(table_name=anomalies_table_name)
    print("✓ Anomalies monitor refreshed")
except Exception as e:
    print(f"Error refreshing anomalies monitor: {str(e)}")

print("\n✓ All monitors refreshed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Monitor Information

# COMMAND ----------

# Get monitor info for all tables
print("=" * 80)
print("MONITOR SUMMARY")
print("=" * 80)

for table_name in [audit_table_name, bpm_table_name, performance_table_name, anomalies_table_name]:
    try:
        monitor_info = lm.get_monitor(table_name=table_name)
        print(f"\n{table_name}:")
        print(f"  Status: {monitor_info.status}")
        print(f"  Profile Metrics Table: {monitor_info.profile_metrics_table_name}")
        print(f"  Drift Metrics Table: {monitor_info.drift_metrics_table_name}")
        print(f"  Schedule: Every 6 hours" if "anomalies" not in table_name else "  Schedule: Every hour")
        print(f"  Custom Metrics: {len(monitor_info.custom_metrics) if monitor_info.custom_metrics else 0}")
    except Exception as e:
        print(f"\n{table_name}: Error getting monitor info - {str(e)}")

print("\n" + "=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Sample Metrics

# COMMAND ----------

# Query the profile metrics table for audit logs
print("Sample metrics from audit logs monitor:\n")

try:
    audit_metrics_table = f"{catalog}.{schema}.audit_logs_profile_metrics"
    
    sample_metrics = spark.sql(f"""
        SELECT 
            window.start as time_window,
            avg_response_time_ms,
            p95_response_time_ms,
            hourly_failure_rate,
            error_rate,
            high_response_time_count,
            unique_services
        FROM {audit_metrics_table}
        WHERE window.start >= current_timestamp() - INTERVAL 7 DAYS
        ORDER BY window.start DESC
        LIMIT 10
    """)
    
    sample_metrics.show(truncate=False)
    
except Exception as e:
    print(f"Metrics table not yet available or no data: {str(e)}")
    print("Run the monitors and wait for the next scheduled refresh.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor Dashboard Queries

# COMMAND ----------

print("Use these queries in Databricks SQL dashboards:\n")

print("1. Audit Logs - Response Time Trends")
print(f"""
SELECT 
    window.start as time_window,
    avg_response_time_ms,
    p95_response_time_ms,
    high_response_time_count
FROM {catalog}.{schema}.audit_logs_profile_metrics
WHERE window.start >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY window.start
""")

print("\n2. BPM Logs - Timeout and Incomplete Trails")
print(f"""
SELECT 
    window.start as time_window,
    timeout_rate * 100 as timeout_rate_percent,
    incomplete_trails_rate * 100 as incomplete_rate_percent,
    avg_total_elapsed_ms
FROM {catalog}.{schema}.bpm_logs_profile_metrics
WHERE window.start >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY window.start
""")

print("\n3. Anomalies - Severity Distribution Over Time")
print(f"""
SELECT 
    window.start as time_window,
    critical_anomaly_rate * 100 as critical_percent,
    high_anomaly_rate * 100 as high_percent,
    unique_anomaly_types
FROM {catalog}.{schema}.anomalies_profile_metrics
WHERE window.start >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY window.start
""")

# COMMAND ----------

print("\n" + "=" * 80)
print("LAKEHOUSE MONITORING SETUP COMPLETE")
print("=" * 80)
print("\nNext steps:")
print("1. View monitors in Databricks UI: Data > [Your Schema] > [Table] > Quality tab")
print("2. Create SQL dashboards using the profile metrics tables")
print("3. Set up alerts based on custom metric thresholds")
print("4. Review monitor status regularly in the Quality tab")
print("=" * 80)

