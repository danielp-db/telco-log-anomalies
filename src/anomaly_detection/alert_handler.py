# Databricks notebook source
"""
Alert Handler
Reads detected anomalies from the anomalies table and triggers alerts/notifications.
Provides summary reports and can integrate with external alerting systems.
"""

# COMMAND ----------

import yaml
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

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
    "anomalies_table": f"{catalog}.{schema}.anomalies",
}

print("Configuration loaded:")
print(f"  Catalog: {config['catalog']}")
print(f"  Schema: {config['schema']}")
print(f"  Anomalies Table: {config['anomalies_table']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Recent Anomalies

# COMMAND ----------

def get_recent_anomalies(config, hours_back=1):
    """
    Retrieve anomalies detected in the last N hours
    """
    anomalies_table = config["anomalies_table"]
    
    # Check if table exists
    try:
        spark.table(anomalies_table)
    except Exception as e:
        print(f"Anomalies table does not exist yet: {anomalies_table}")
        return None
    
    cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
    
    recent_anomalies = (spark.table(anomalies_table)
        .filter(col("detected_at") >= lit(cutoff_time))
    )
    
    count = recent_anomalies.count()
    print(f"\nFound {count} anomalies detected in the last {hours_back} hour(s)")
    
    return recent_anomalies if count > 0 else None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Alert Summary

# COMMAND ----------

def generate_alert_summary(anomalies_df):
    """
    Generate a comprehensive summary of detected anomalies
    """
    print("\n" + "="*80)
    print("ANOMALY ALERT SUMMARY")
    print(f"Generated at: {datetime.utcnow().isoformat()}")
    print("="*80)
    
    # Overall summary by severity
    print("\n--- SUMMARY BY SEVERITY ---")
    severity_summary = (anomalies_df
        .groupBy("severity")
        .agg(count("*").alias("count"))
        .orderBy(
            when(col("severity") == "CRITICAL", 1)
            .when(col("severity") == "HIGH", 2)
            .when(col("severity") == "MEDIUM", 3)
            .when(col("severity") == "LOW", 4)
            .otherwise(5)
        )
    )
    severity_summary.show(truncate=False)
    
    # Summary by anomaly type
    print("\n--- SUMMARY BY ANOMALY TYPE ---")
    type_summary = (anomalies_df
        .groupBy("anomaly_type", "severity")
        .agg(count("*").alias("count"))
        .orderBy("severity", "anomaly_type")
    )
    type_summary.show(truncate=False)
    
    # Summary by affected service
    print("\n--- TOP AFFECTED SERVICES ---")
    service_summary = (anomalies_df
        .filter(col("affected_service").isNotNull())
        .groupBy("affected_service")
        .agg(
            count("*").alias("anomaly_count"),
            countDistinct("anomaly_type").alias("anomaly_types"),
            max("severity").alias("max_severity")
        )
        .orderBy(col("anomaly_count").desc())
        .limit(10)
    )
    service_summary.show(truncate=False)
    
    # Critical anomalies detail
    critical = anomalies_df.filter(col("severity") == "CRITICAL")
    critical_count = critical.count()
    
    if critical_count > 0:
        print(f"\n--- CRITICAL ANOMALIES ({critical_count}) ---")
        critical.select(
            "anomaly_type",
            "affected_service",
            "metric_name",
            "metric_value",
            "threshold_value",
            "details"
        ).show(truncate=False)
    
    print("\n" + "="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Detailed Alert Messages

# COMMAND ----------

def create_alert_messages(anomalies_df):
    """
    Create detailed alert messages for each severity level
    """
    alerts = {}
    
    # Group by severity
    for severity in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
        severity_anomalies = anomalies_df.filter(col("severity") == severity).collect()
        
        if severity_anomalies:
            messages = []
            for anomaly in severity_anomalies:
                msg = {
                    "id": anomaly.anomaly_id,
                    "type": anomaly.anomaly_type,
                    "severity": anomaly.severity,
                    "service": anomaly.affected_service,
                    "instance": anomaly.affected_instance,
                    "cluster": anomaly.affected_cluster,
                    "metric": f"{anomaly.metric_name}={anomaly.metric_value:.2f}" if anomaly.metric_value else None,
                    "threshold": anomaly.threshold_value,
                    "baseline": anomaly.baseline_value,
                    "details": anomaly.details,
                    "time_window": f"{anomaly.time_window_start} to {anomaly.time_window_end}",
                    "detected_at": anomaly.detected_at
                }
                messages.append(msg)
            
            alerts[severity] = messages
    
    return alerts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Format Alert Output

# COMMAND ----------

def format_alert_for_display(alert):
    """
    Format a single alert for display
    """
    lines = []
    lines.append(f"[{alert['severity']}] {alert['type']}")
    lines.append(f"  Service: {alert['service'] or 'N/A'}")
    if alert['instance']:
        lines.append(f"  Instance: {alert['instance']}")
    if alert['cluster']:
        lines.append(f"  Cluster: {alert['cluster']}")
    if alert['metric']:
        lines.append(f"  Metric: {alert['metric']}")
    if alert['threshold']:
        lines.append(f"  Threshold: {alert['threshold']:.2f}")
    if alert['baseline']:
        lines.append(f"  Baseline: {alert['baseline']:.2f}")
    lines.append(f"  Details: {alert['details']}")
    lines.append(f"  Time Window: {alert['time_window']}")
    lines.append(f"  Detected At: {alert['detected_at']}")
    lines.append("")
    
    return "\n".join(lines)

# COMMAND ----------

def display_alerts(alerts):
    """
    Display formatted alerts grouped by severity
    """
    print("\n" + "="*80)
    print("DETAILED ALERT MESSAGES")
    print("="*80)
    
    for severity in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
        if severity in alerts and alerts[severity]:
            print(f"\n{'#'*80}")
            print(f"# {severity} SEVERITY ALERTS ({len(alerts[severity])})")
            print(f"{'#'*80}\n")
            
            for alert in alerts[severity]:
                print(format_alert_for_display(alert))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Send Notifications (Placeholder)

# COMMAND ----------

def send_notifications(alerts, config):
    """
    Send notifications via configured channels
    
    This is a placeholder for actual notification integration.
    In production, this could integrate with:
    - Databricks Jobs API for email notifications
    - Slack webhooks
    - PagerDuty
    - Microsoft Teams
    - Custom webhook endpoints
    """
    print("\n" + "="*80)
    print("SENDING NOTIFICATIONS")
    print("="*80)
    
    # Count alerts by severity
    critical_count = len(alerts.get("CRITICAL", []))
    high_count = len(alerts.get("HIGH", []))
    medium_count = len(alerts.get("MEDIUM", []))
    low_count = len(alerts.get("LOW", []))
    
    print(f"\nAlert counts:")
    print(f"  CRITICAL: {critical_count}")
    print(f"  HIGH: {high_count}")
    print(f"  MEDIUM: {medium_count}")
    print(f"  LOW: {low_count}")
    
    # In a real implementation, you would send notifications here
    # Example integrations:
    
    # 1. Databricks notification (using dbutils.notebook.exit for job notifications)
    if critical_count > 0:
        print(f"\n[INTEGRATION] Would send CRITICAL alert to on-call team")
    
    if high_count > 0:
        print(f"[INTEGRATION] Would send HIGH alert to operations team")
    
    if medium_count > 0 or low_count > 0:
        print(f"[INTEGRATION] Would log MEDIUM/LOW alerts to monitoring dashboard")
    
    # 2. Example webhook payload (not actually sent)
    if critical_count > 0 or high_count > 0:
        webhook_payload = {
            "timestamp": datetime.utcnow().isoformat(),
            "summary": f"Log Anomaly Detection: {critical_count} CRITICAL, {high_count} HIGH alerts",
            "critical_count": critical_count,
            "high_count": high_count,
            "alerts": []
        }
        
        # Add critical alerts to payload
        for alert in alerts.get("CRITICAL", []):
            webhook_payload["alerts"].append({
                "severity": "CRITICAL",
                "type": alert["type"],
                "service": alert["service"],
                "details": alert["details"]
            })
        
        print(f"\n[INTEGRATION] Webhook payload prepared:")
        print(f"  URL: https://example.com/webhook/anomaly-alerts")
        print(f"  Payload size: {len(str(webhook_payload))} bytes")
    
    print("\n[NOTE] Notification integration placeholders shown above.")
    print("[NOTE] In production, integrate with your notification system of choice.")
    print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Alert Dashboard Query

# COMMAND ----------

def create_dashboard_summary(config):
    """
    Create SQL queries for dashboard visualization
    """
    print("\n" + "="*80)
    print("DASHBOARD QUERIES")
    print("="*80)
    
    anomalies_table = config["anomalies_table"]
    
    # Query 1: Anomalies over time
    print("\n--- Query 1: Anomalies Over Time (Last 24 Hours) ---")
    query1 = f"""
    SELECT 
        date_trunc('hour', detected_at) as hour,
        anomaly_type,
        severity,
        COUNT(*) as count
    FROM {anomalies_table}
    WHERE detected_at >= current_timestamp() - INTERVAL 24 HOURS
    GROUP BY date_trunc('hour', detected_at), anomaly_type, severity
    ORDER BY hour DESC, count DESC
    """
    print(query1)
    
    try:
        result1 = spark.sql(query1)
        print(f"\nResults (showing last 10):")
        result1.limit(10).show(truncate=False)
    except Exception as e:
        print(f"Query not executed (table may be empty): {str(e)}")
    
    # Query 2: Top affected services
    print("\n--- Query 2: Top Affected Services (Last 7 Days) ---")
    query2 = f"""
    SELECT 
        affected_service,
        COUNT(*) as total_anomalies,
        SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) as critical,
        SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) as high,
        SUM(CASE WHEN severity = 'MEDIUM' THEN 1 ELSE 0 END) as medium,
        MAX(detected_at) as last_anomaly
    FROM {anomalies_table}
    WHERE detected_at >= current_timestamp() - INTERVAL 7 DAYS
    GROUP BY affected_service
    ORDER BY total_anomalies DESC
    LIMIT 20
    """
    print(query2)
    
    try:
        result2 = spark.sql(query2)
        print(f"\nResults:")
        result2.show(truncate=False)
    except Exception as e:
        print(f"Query not executed (table may be empty): {str(e)}")
    
    # Query 3: Recent critical anomalies
    print("\n--- Query 3: Recent Critical Anomalies ---")
    query3 = f"""
    SELECT 
        detected_at,
        anomaly_type,
        affected_service,
        affected_cluster,
        metric_name,
        metric_value,
        threshold_value,
        details
    FROM {anomalies_table}
    WHERE severity = 'CRITICAL'
        AND detected_at >= current_timestamp() - INTERVAL 24 HOURS
    ORDER BY detected_at DESC
    """
    print(query3)
    
    try:
        result3 = spark.sql(query3)
        print(f"\nResults:")
        result3.show(truncate=False)
    except Exception as e:
        print(f"Query not executed (table may be empty): {str(e)}")
    
    print("\n" + "="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Alert Handler Execution

# COMMAND ----------

def handle_alerts(config):
    """
    Main function to handle all alert processing
    """
    print("\n" + "="*80)
    print("ALERT HANDLER STARTING")
    print(f"Execution Time: {datetime.utcnow().isoformat()}")
    print("="*80)
    
    # Get recent anomalies (last 1 hour, matching the detection job schedule)
    anomalies_df = get_recent_anomalies(config, hours_back=1)
    
    if anomalies_df is None or anomalies_df.count() == 0:
        print("\nNo recent anomalies found. Nothing to alert on.")
        print("="*80)
        return
    
    # Generate summary
    generate_alert_summary(anomalies_df)
    
    # Create detailed alert messages
    alerts = create_alert_messages(anomalies_df)
    
    # Display alerts
    display_alerts(alerts)
    
    # Send notifications
    send_notifications(alerts, config)
    
    # Create dashboard queries
    create_dashboard_summary(config)
    
    print("\n" + "="*80)
    print("ALERT HANDLER COMPLETED SUCCESSFULLY")
    print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Alert Handler

# COMMAND ----------

# Run the alert handler
handle_alerts(config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Mark Anomalies as Acknowledged

# COMMAND ----------

def acknowledge_anomalies(anomaly_ids, config):
    """
    Mark specific anomalies as acknowledged
    This is useful for tracking which alerts have been reviewed
    """
    if not anomaly_ids:
        return
    
    anomalies_table = config["anomalies_table"]
    
    # Read current table
    df = spark.table(anomalies_table)
    
    # Add acknowledged flag if it doesn't exist
    if "acknowledged" not in df.columns:
        df = df.withColumn("acknowledged", lit(False))
        df = df.withColumn("acknowledged_at", lit(None).cast("timestamp"))
        df = df.withColumn("acknowledged_by", lit(None).cast("string"))
    
    # Update acknowledged status
    updated_df = df.withColumn(
        "acknowledged",
        when(col("anomaly_id").isin(anomaly_ids), lit(True))
        .otherwise(col("acknowledged"))
    ).withColumn(
        "acknowledged_at",
        when(col("anomaly_id").isin(anomaly_ids), current_timestamp())
        .otherwise(col("acknowledged_at"))
    )
    
    # Overwrite the table
    (updated_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(anomalies_table)
    )
    
    print(f"Marked {len(anomaly_ids)} anomalies as acknowledged")

# Example usage (commented out):
# acknowledge_anomalies(["anomaly-id-1", "anomaly-id-2"], config)

