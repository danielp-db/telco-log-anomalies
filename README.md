# Log Anomaly Detection System

A comprehensive Databricks-based solution for real-time log monitoring and anomaly detection across multiple log types (audit, BPM, and performance tracking logs).

## Project Overview

This system provides:
1. **Mock Log Generation**: Continuously generates realistic log files with injected anomalies
2. **Streaming Ingestion**: Real-time processing of log files into Delta tables
3. **Anomaly Detection**: Hourly analysis detecting 6+ types of anomalies
4. **Alert Management**: Automated alerting with severity-based notifications

## Architecture

```
┌─────────────────────┐
│  Log Generator      │ → Generates mock logs every 5s
│  (Workflow 1)       │ → Writes to Databricks Volume
└──────────┬──────────┘
           │
           ├─── /audit/*.json
           ├─── /bpm/*.json
           └─── /performance/*.json
                      │
                      ↓
           ┌──────────────────────┐
           │ Streaming Ingestion  │ → Reads from Volume
           │  (Workflow 2)        │ → Parses JSON logs
           │  - Audit logs        │ → Writes to Delta tables
           │  - BPM logs          │
           │  - Performance logs  │
           └──────────┬───────────┘
                      │
                      ↓
              ┌──────────────┐
              │ Delta Tables │
              │  - audit_logs      │
              │  - bpm_logs        │
              │  - performance_logs│
              └──────────┬─────────┘
                         │
                         ↓ (Every hour)
              ┌────────────────────┐
              │ Anomaly Detection  │ → Analyzes patterns
              │  (Workflow 3)      │ → Detects anomalies
              │  - Detector        │ → Stores results
              │  - Alert Handler   │ → Sends notifications
              └────────────────────┘
                         │
                         ↓
              ┌──────────────────┐
              │ Anomalies Table  │
              │ + Alerts/Dashboards│
              └──────────────────┘
```

## Project Structure

```
LogAnomalyDetection/
├── databricks.yml                      # Databricks Asset Bundle config
├── config/
│   └── common_config.yml               # Shared YAML configuration
├── src/
│   ├── log_generator/
│   │   └── generator_notebook.py       # Mock log generator
│   ├── streaming_ingestion/
│   │   ├── ingest_audit_logs.py        # Audit log ingestion
│   │   ├── ingest_bpm_logs.py          # BPM log ingestion
│   │   └── ingest_performance_logs.py  # Performance log ingestion
│   └── anomaly_detection/
│       ├── anomaly_detector.py         # Anomaly detection logic
│       └── alert_handler.py            # Alert handling & notifications
└── data_templates/                     # Log templates for generation
    ├── audit/
    ├── bpm/
    └── performance_tracking/
```

## Anomaly Detection Capabilities

The system detects 6+ types of anomalies:

### 1. Failure Rate Spike Detection
- Compares current failure rates vs. 7-day baseline
- Alerts when failures exceed 2x normal rate
- Groups by transaction/service name

### 2. Missing Heartbeat Detection
- Tracks expected periodic log patterns
- Alerts when no logs received for 5+ minutes
- Monitors all services across log types

### 3. High Response Time Detection
- Calculates P95 and P99 response times
- Alerts when P95 > 30s or P99 > 60s
- Tracks both audit and performance logs

### 4. Transaction Timeout Detection
- Identifies incomplete transactions
- Detects operations that started but never finished
- Tracks timeout patterns by service

### 5. Log Volume Anomaly Detection
- Monitors log volume changes
- Alerts on 3x spikes or significant drops
- Compares against historical patterns

### 6. Service Degradation Detection
- Tracks gradual performance degradation
- Alerts when response times increase >50%
- Identifies trending performance issues

## Getting Started

### Prerequisites

- Databricks workspace with access to:
  - Unity Catalog (or Hive Metastore)
  - Volumes for file storage
  - Workflows/Jobs API
- Appropriate cluster permissions

### Configuration

1. **Update `databricks.yml`** with your workspace details:
   - Set the correct workspace host URL
   - Configure target environments (dev/prod)
   - Adjust cluster sizes as needed

2. **Configure parameters** in `config/common_config.yml`:
   - Log generation settings (interval, batch size, anomaly injection rate)
   - Streaming settings (files per trigger, processing time)
   - Anomaly detection thresholds (failure rate, response time, heartbeat interval)
   - Time windows (short, medium, long, baseline)
   - Alert severity levels

### Deployment

Using Databricks Asset Bundles:

```bash
# Validate the bundle
databricks bundle validate

# Deploy to dev environment
databricks bundle deploy -t dev

# Run individual jobs
databricks bundle run log_generator_job -t dev
databricks bundle run streaming_ingestion_job -t dev
databricks bundle run anomaly_detection_job -t dev
```

### Manual Setup (Alternative)

1. **Create the Volume**:
   ```sql
   CREATE VOLUME IF NOT EXISTS main.log_anomaly_dev.raw_logs;
   ```

2. **Upload notebooks** to your workspace:
   - Upload all files from `src/` to `/Workspace/src/`
   - Upload `config/common_config.py` to `/Workspace/config/`

3. **Create workflows** manually in the Databricks UI using the job definitions in `databricks.yml`

## Usage

### Starting the System

1. **Start Log Generator**: Runs continuously, generates logs every 5 seconds
   ```
   Run: log_generator_job
   ```

2. **Start Streaming Ingestion**: Runs continuously, processes incoming logs
   ```
   Run: streaming_ingestion_job
   ```

3. **Anomaly Detection**: Runs hourly (automated via schedule)
   ```
   Run: anomaly_detection_job
   ```

### Monitoring

**View Ingested Logs:**
```sql
SELECT * FROM main.log_anomaly_dev.audit_logs ORDER BY log_timestamp DESC LIMIT 100;
SELECT * FROM main.log_anomaly_dev.bpm_logs ORDER BY log_timestamp DESC LIMIT 100;
SELECT * FROM main.log_anomaly_dev.performance_logs ORDER BY log_timestamp DESC LIMIT 100;
```

**View Detected Anomalies:**
```sql
SELECT * FROM main.log_anomaly_dev.anomalies 
ORDER BY detected_at DESC 
LIMIT 50;
```

**Anomalies by Severity:**
```sql
SELECT severity, anomaly_type, COUNT(*) as count
FROM main.log_anomaly_dev.anomalies
WHERE detected_at >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY severity, anomaly_type
ORDER BY severity, count DESC;
```

**Top Affected Services:**
```sql
SELECT affected_service, COUNT(*) as anomaly_count
FROM main.log_anomaly_dev.anomalies
WHERE detected_at >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY affected_service
ORDER BY anomaly_count DESC
LIMIT 10;
```

### Dashboard Queries

The alert handler generates dashboard queries automatically. You can also create Databricks SQL dashboards using:

1. Anomalies over time (time series chart)
2. Severity distribution (pie chart)
3. Top affected services (bar chart)
4. Recent critical anomalies (table)

## Configuration Parameters

All configuration is now centralized in `config/common_config.yml`:

### Database Settings
```yaml
database:
  catalog: "main"           # Unity Catalog name
  schema: "log_anomaly_db"  # Schema name
  volume_name: "raw_logs"   # Volume name
```

### Log Generation
```yaml
log_generation:
  interval_seconds: 5        # Generate logs every 5 seconds
  logs_per_batch: 50         # Logs per type per batch
  anomaly_injection_rate: 0.05  # 5% anomaly injection
```

### Streaming Settings
```yaml
streaming:
  max_files_per_trigger: 10
  trigger_processing_time: "10 seconds"
```

### Anomaly Detection Thresholds
```yaml
anomaly_detection:
  failure_rate_threshold_multiplier: 2.0      # Alert at 2x normal
  high_response_time_p95_threshold_ms: 30000  # 30 seconds
  high_response_time_p99_threshold_ms: 60000  # 60 seconds
  missing_heartbeat_interval_minutes: 5
  volume_anomaly_threshold_multiplier: 3.0
  degradation_increase_threshold_percent: 50
```

### Time Windows
```yaml
time_windows:
  short_window_minutes: 5
  medium_window_minutes: 15
  long_window_minutes: 30
  baseline_window_days: 7
```

## Alert Integration

The alert handler provides placeholders for notification integration. To add real notifications:

1. **Databricks Job Notifications**: Configure in the workflow settings
2. **Webhook Integration**: Add webhook calls in `alert_handler.py`
3. **Email/Slack**: Use Databricks notification destinations
4. **PagerDuty**: Integrate via webhook or API

Example webhook integration:
```python
import requests

def send_webhook_alert(alerts, webhook_url):
    payload = {
        "severity": "CRITICAL",
        "count": len(alerts),
        "alerts": [{"service": a["service"], "details": a["details"]} for a in alerts]
    }
    requests.post(webhook_url, json=payload)
```

## Troubleshooting

### Logs Not Appearing in Delta Tables
- Check if the streaming jobs are running
- Verify Volume paths are correct
- Check checkpoint locations for errors
- Review Spark UI for streaming query status

### No Anomalies Detected
- Ensure logs have been ingested for at least 7 days (baseline period)
- Check if thresholds are too high
- Verify anomaly injection is working in the generator
- Review detection logic for errors

### Performance Issues
- Increase cluster size for streaming jobs
- Adjust `maxFilesPerTrigger` parameter
- Optimize Delta table with `OPTIMIZE` and `VACUUM`
- Consider partitioning large tables

## Maintenance

### Regular Tasks
- **Optimize Delta Tables**: Run weekly
  ```sql
  OPTIMIZE main.log_anomaly_dev.audit_logs;
  OPTIMIZE main.log_anomaly_dev.bpm_logs;
  OPTIMIZE main.log_anomaly_dev.performance_logs;
  OPTIMIZE main.log_anomaly_dev.anomalies;
  ```

- **Clean Up Old Data**: Remove logs older than retention period
  ```sql
  DELETE FROM main.log_anomaly_dev.audit_logs 
  WHERE log_timestamp < current_timestamp() - INTERVAL 90 DAYS;
  ```

- **Vacuum Delta Tables**: After deletions
  ```sql
  VACUUM main.log_anomaly_dev.audit_logs RETAIN 168 HOURS;
  ```

## Production Considerations

1. **Security**: Use service principals for authentication
2. **Monitoring**: Set up job failure alerts
3. **Scaling**: Adjust cluster sizes based on log volume
4. **Cost Optimization**: Use spot instances for non-critical workloads
5. **Data Retention**: Define and enforce retention policies
6. **Backup**: Regular backups of anomalies table
7. **Testing**: Test with production-like data volumes

## License

This project is provided as-is for educational and development purposes.

## Support

For issues or questions:
1. Check the Databricks workspace logs
2. Review streaming query status in Spark UI
3. Examine Delta table history for issues
4. Check job run history in Databricks UI

