# Databricks notebook source
"""
Mock Log Generator
Continuously generates realistic log files based on templates and streams them to a Databricks Volume.
Injects various types of anomalies for testing the anomaly detection system.
"""

# COMMAND ----------

# MAGIC %pip install faker pyyaml

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import random
import time
import uuid
import yaml
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker
fake = Faker()

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
    "volume_base_path": f"/Volumes/{catalog}/{schema}/{volume_name}",
    "audit_logs_path": f"/Volumes/{catalog}/{schema}/{volume_name}/audit",
    "bpm_logs_path": f"/Volumes/{catalog}/{schema}/{volume_name}/bpm",
    "performance_logs_path": f"/Volumes/{catalog}/{schema}/{volume_name}/performance",
    "log_generation_interval_seconds": yaml_config["log_generation"]["interval_seconds"],
    "logs_per_batch": yaml_config["log_generation"]["logs_per_batch"],
    "anomaly_injection_rate": yaml_config["log_generation"]["anomaly_injection_rate"],
}

print("Configuration loaded:")
print(f"  Catalog: {config['catalog']}")
print(f"  Schema: {config['schema']}")
print(f"  Volume: {config['volume_name']}")
print(f"  Base Path: {config['volume_base_path']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Template Loaders

# COMMAND ----------

def load_audit_template():
    """Load audit log template structure"""
    return {
        "timeMillis": None,
        "thread": None,
        "level": "INFO",
        "loggerName": "audit",
        "message": {
            "OriginalMessageId": "",
            "Vtier": "q100csg140c3",
            "ClientIP": None,
            "RequestURL": None,
            "SourceClass": "com.att.csi.gateway2.ResponseHandler",
            "OriginationSystemId": None,
            "OriginationSystemVersion": "217",
            "OriginationSystemName": "csitest",
            "SourceMethod": "",
            "TransactionName": None,
            "TransactionStatus": "COMPLETE",
            "HostIPAddress": None,
            "FaultTimestamp": None,
            "FaultEntity": None,
            "InitiatedTimestamp": None,
            "ElapsedTime": None,
            "Subject": None,
            "HostName": None,
            "ResponseCode": "0",
            "Description": None,
            "Mode": "",
            "HttpMethod": None,
            "Cluster": "Q27B",
            "ServiceKeyData1": "",
            "ServiceKeyData2": "NA",
            "ClientApp": "",
            "ExternalFaultDescription": None,
            "FaultSequenceNumber": None,
            "FaultLevel": None,
            "ExternalFaultCode": None,
            "ConversationId": None,
            "UniqueTransactionId": None,
            "OriginatorId": "",
            "ApplicationId": "csitest",
            "FaultCode": None,
            "FaultDescription": None,
            "InstanceName": None,
            "ResponseDescription": "Success"
        }
    }

def load_performance_template():
    """Load performance tracking log template structure"""
    return {
        "timeMillis": None,
        "thread": None,
        "level": "DEBUG",
        "loggerName": "performance.tracking",
        "message": {
            "ConversationId": None,
            "ClientApp": None,
            "ClientDME2Lookup": None,
            "Mode": "",
            "Service": None,
            "CustomTag": "BOBPM",
            "TrailMarks": [],
            "Cluster": "AE2P01",
            "Token": "",
            "Vtier": "N/A",
            "TransactionId": None,
            "InstanceName": None,
            "HostIPAddress": None,
            "ClientId": "BOBPM",
            "StartTimeStamp": None,
            "Application": "DUCSDB Adapter",
            "ReqMsgSize": None,
            "RespMsgSize": 222,
            "HostName": None
        },
        "endOfBatch": False,
        "loggerFqcn": "org.apache.logging.slf4j.Log4jLogger",
        "contextMap": {
            "service.name": "csbobpm-ACSI_DUCSDB",
            "span_id": None,
            "trace_flags": "00",
            "trace_id": None
        },
        "threadId": None,
        "threadPriority": 5
    }

def load_bpm_template():
    """Load BPM log template structure"""
    return {
        "timeMillis": None,
        "thread": None,
        "level": "DEBUG",
        "loggerName": "performance.tracking",
        "message": {
            "ConversationId": None,
            "ResponseCode": "0",
            "ClientApp": "",
            "Mode": "",
            "Service": None,
            "CustomTag": "csitest",
            "TrailMarks": [],
            "Cluster": "Q27B",
            "Vtier": "q100csg140c3",
            "TransactionId": None,
            "InstanceName": None,
            "HostIPAddress": None,
            "ServiceKeyData2": "NA",
            "StartTimeStamp": None,
            "ServiceKeyData1": "",
            "Application": "ServiceGateway",
            "ReqMsgSize": "3012",
            "RespMsgSize": "2048",
            "HostName": None
        }
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generators with Anomaly Injection

# COMMAND ----------

class AnomalyInjector:
    """Controls when and what type of anomalies to inject"""
    
    def __init__(self, injection_rate=0.05):
        self.injection_rate = injection_rate
        self.anomaly_types = [
            "high_response_time",
            "failure",
            "timeout",
            "missing_heartbeat"
        ]
    
    def should_inject_anomaly(self):
        """Randomly decide if we should inject an anomaly"""
        return random.random() < self.injection_rate
    
    def get_anomaly_type(self):
        """Randomly select an anomaly type"""
        return random.choice(self.anomaly_types)

# COMMAND ----------

class LogGenerator:
    """Generates realistic log entries with anomalies"""
    
    def __init__(self, config):
        self.config = config
        self.injector = AnomalyInjector(config["anomaly_injection_rate"])
        self.transaction_names = [
            "AddWirelineTroubleReport",
            "ProcessOrderNotification",
            "OrderManagementRules",
            "ProcessVehicleSubscriberNotification",
            "SendLatePaymentNotificationAsync"
        ]
        self.service_names = [
            "ProcessOrderNotification",
            "OrderManagementRules",
            "ProcessOrderNotificationForEach",
            "ProcessVehicleSubscriberNotification"
        ]
        self.services_last_heartbeat = {}
    
    def generate_audit_log(self, inject_anomaly=False, anomaly_type=None):
        """Generate a single audit log entry"""
        log = load_audit_template()
        now = datetime.utcnow()
        
        # Basic fields
        log["timeMillis"] = int(now.timestamp() * 1000)
        log["thread"] = f"DME2JMS::ListenerThread[TempQueue]-{random.randint(1, 50)}"
        
        # Message fields
        transaction = random.choice(self.transaction_names)
        log["message"]["TransactionName"] = transaction
        log["message"]["HostIPAddress"] = fake.ipv4_private()
        log["message"]["HostName"] = fake.hostname()
        log["message"]["ConversationId"] = f"csitest~CNG-CSI~{uuid.uuid4()}"
        log["message"]["UniqueTransactionId"] = f"ServiceGateway782594@q100csg140c3_{uuid.uuid4()}"
        log["message"]["InstanceName"] = f"ServiceGateway~782594@q100csg140c3~Q27B"
        log["message"]["Subject"] = f"CW.pub.spm2.{transaction.lower()}.response"
        
        # Timestamps
        initiated = now - timedelta(milliseconds=random.randint(10000, 20000))
        log["message"]["InitiatedTimestamp"] = initiated.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
        log["message"]["FaultTimestamp"] = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
        
        # Inject anomalies
        if inject_anomaly:
            if anomaly_type == "failure":
                log["level"] = "ERROR"
                log["message"]["ResponseCode"] = str(random.choice([500, 503, 504]))
                log["message"]["TransactionStatus"] = "FAILED"
                log["message"]["ResponseDescription"] = "Service Error"
                log["message"]["ElapsedTime"] = str(random.randint(5000, 15000))
            elif anomaly_type == "high_response_time":
                # Very slow response
                log["message"]["ElapsedTime"] = str(random.randint(35000, 90000))
            elif anomaly_type == "timeout":
                log["message"]["ResponseCode"] = "408"
                log["message"]["TransactionStatus"] = "TIMEOUT"
                log["message"]["ResponseDescription"] = "Request Timeout"
                log["message"]["ElapsedTime"] = str(random.randint(60000, 120000))
        else:
            # Normal response time
            log["message"]["ElapsedTime"] = str(random.randint(11000, 17000))
        
        return log
    
    def generate_performance_log(self, inject_anomaly=False, anomaly_type=None):
        """Generate a single performance tracking log entry"""
        log = load_performance_template()
        now = datetime.utcnow()
        
        # Basic fields
        log["timeMillis"] = int(now.timestamp() * 1000)
        log["thread"] = f"com.att.adapter.hydra.ServiceAdapterHandler-Thread-{random.randint(1, 150)}"
        log["threadId"] = random.randint(100, 200)
        
        # Message fields
        service = random.choice(self.service_names)
        log["message"]["Service"] = service
        log["message"]["ConversationId"] = f"spm2async~CNG-CSI~{uuid.uuid4()}-0"
        log["message"]["ClientApp"] = f"M2E~bobpmorders-{fake.slug()}"
        log["message"]["ClientDME2Lookup"] = f"dme2://DME2SEARCH/service=com.att.csi.m2e.DUCSDBAdapter/version=1012/envContext=PROD?Partner=BOBPM&StickyKey=AE2P01"
        log["message"]["TransactionId"] = f"COMS{random.randint(1000000000, 9999999999)}"
        log["message"]["InstanceName"] = f"acsi-ducsdb-1012-5-4-ae2p01-{fake.slug()}"
        log["message"]["HostIPAddress"] = fake.ipv4_private()
        log["message"]["HostName"] = fake.hostname()
        log["message"]["StartTimeStamp"] = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
        log["message"]["ReqMsgSize"] = random.randint(500, 2000)
        log["contextMap"]["span_id"] = fake.sha1()[:16]
        log["contextMap"]["trace_id"] = fake.sha1()
        
        # Trail marks
        if inject_anomaly and anomaly_type == "timeout":
            # Incomplete transaction
            start = now - timedelta(milliseconds=random.randint(60000, 120000))
            log["message"]["TrailMarks"] = [
                {
                    "Name": "AdapterHelper",
                    "Detail": "insertTransactionHistory",
                    "StartTime": start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z",
                    "EndTime": None,
                    "Complete": False,
                    "ElapsedDuration": None
                }
            ]
        elif inject_anomaly and anomaly_type == "high_response_time":
            # Very slow operation
            duration = random.randint(40000, 100000)
            start = now - timedelta(milliseconds=duration)
            log["message"]["TrailMarks"] = [
                {
                    "Name": "AdapterHelper",
                    "Detail": "insertTransactionHistory",
                    "StartTime": start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z",
                    "EndTime": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z",
                    "Complete": True,
                    "ElapsedDuration": duration
                }
            ]
        else:
            # Normal operation
            duration = random.randint(3, 15)
            start = now - timedelta(milliseconds=duration)
            log["message"]["TrailMarks"] = [
                {
                    "Name": "AdapterHelper",
                    "Detail": "insertTransactionHistory",
                    "StartTime": start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z",
                    "EndTime": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z",
                    "Complete": True,
                    "ElapsedDuration": duration
                }
            ]
        
        return log
    
    def generate_bpm_log(self, inject_anomaly=False, anomaly_type=None):
        """Generate a single BPM log entry"""
        log = load_bpm_template()
        now = datetime.utcnow()
        
        # Basic fields
        log["timeMillis"] = int(now.timestamp() * 1000)
        log["thread"] = f"DME2JMS::ListenerThread[TempQueue]-{random.randint(1, 50)}"
        
        # Message fields
        service = random.choice(self.transaction_names)
        log["message"]["Service"] = service
        log["message"]["ConversationId"] = f"csitest~CNG-CSI~{uuid.uuid4()}"
        log["message"]["TransactionId"] = f"ServiceGateway782594@q100csg140c3_{uuid.uuid4()}"
        log["message"]["InstanceName"] = f"ServiceGateway~782594@q100csg140c3~Q27B"
        log["message"]["HostIPAddress"] = fake.ipv4_private()
        log["message"]["HostName"] = fake.hostname()
        log["message"]["StartTimeStamp"] = "1970-01-01T00:00:00.001Z"
        
        # Trail marks
        if inject_anomaly and anomaly_type == "high_response_time":
            duration = random.randint(35000, 90000)
        else:
            duration = random.randint(20000, 35000)
        
        start = now - timedelta(milliseconds=duration)
        log["message"]["TrailMarks"] = [
            {
                "Name": "Main",
                "Detail": "",
                "StartTime": start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z",
                "EndTime": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z",
                "Complete": True if not (inject_anomaly and anomaly_type == "timeout") else False,
                "ElapsedDuration": duration
            }
        ]
        
        if inject_anomaly and anomaly_type == "failure":
            log["message"]["ResponseCode"] = str(random.choice([500, 503, 504]))
        
        return log

# COMMAND ----------

# MAGIC %md
# MAGIC ## File Writing Functions

# COMMAND ----------

def write_logs_to_volume(logs, log_type, config):
    """
    Write a batch of logs to the appropriate volume path using dbutils
    
    Args:
        logs: List of log dictionaries
        log_type: Type of logs ('audit', 'bpm', 'performance')
        config: Configuration dictionary
    """

    # Determine the path based on log type
    if log_type == "audit":
        path = config["audit_logs_path"]
    elif log_type == "bpm":
        path = config["bpm_logs_path"]
    elif log_type == "performance":
        path = config["performance_logs_path"]
    else:
        raise ValueError(f"Unknown log type: {log_type}")
    
    # Create directory if it doesn't exist using dbutils
    dbutils.fs.mkdirs(path)
    
    # Generate filename with timestamp
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"{log_type}_logs_{timestamp}.json"
    filepath = f"{path}/{filename}"
    
    # Write logs as JSONL (JSON Lines format) using dbutils
    log_lines = "\n".join([json.dumps(log) for log in logs])
    dbutils.fs.put(filepath, log_lines, overwrite=True)
    
    return filepath

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Generation Loop

# COMMAND ----------

def generate_and_stream_logs(config):
    """
    Continuously generate and stream logs to the volume
    """
    generator = LogGenerator(config)
    batch_count = 0
    
    print(f"Starting log generation...")
    print(f"  Interval: {config['log_generation_interval_seconds']} seconds")
    print(f"  Logs per batch: {config['logs_per_batch']}")
    print(f"  Anomaly injection rate: {config['anomaly_injection_rate'] * 100}%")
    print(f"  Output path: {config['volume_base_path']}")
    print()
    
    try:
        while True:
            batch_count += 1
            start_time = time.time()
            
            # Generate each type of log
            for log_type, generator_func in [
                ("audit", generator.generate_audit_log),
                ("performance", generator.generate_performance_log),
                ("bpm", generator.generate_bpm_log)
            ]:
                logs = []
                anomaly_count = 0
                
                for i in range(config["logs_per_batch"]):
                    inject_anomaly = generator.injector.should_inject_anomaly()
                    anomaly_type = generator.injector.get_anomaly_type() if inject_anomaly else None
                    
                    if inject_anomaly:
                        anomaly_count += 1
                    
                    log = generator_func(inject_anomaly=inject_anomaly, anomaly_type=anomaly_type)
                    logs.append(log)
                
                # Write logs to volume
                filepath = write_logs_to_volume(logs, log_type, config)
                
                print(f"[Batch {batch_count}] {log_type.upper()}: Wrote {len(logs)} logs ({anomaly_count} anomalies) to {filepath}")
            
            # Calculate time spent and sleep
            elapsed = time.time() - start_time
            sleep_time = max(0, config["log_generation_interval_seconds"] - elapsed)
            
            if batch_count % 10 == 0:
                print(f"\n--- Generated {batch_count} batches, sleeping for {sleep_time:.2f}s ---\n")
            
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        print("\nLog generation stopped by user.")
    except Exception as e:
        print(f"\nError during log generation: {str(e)}")
        raise

# COMMAND ----------

# Start the continuous generation
generate_and_stream_logs(config)

