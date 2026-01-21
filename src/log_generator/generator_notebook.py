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
    "bpm_adapter_logs_path": f"/Volumes/{catalog}/{schema}/{volume_name}/bpm_adapter",
    "bpm_audit_logs_path": f"/Volumes/{catalog}/{schema}/{volume_name}/bpm_audit",
    "bpm_perf_logs_path": f"/Volumes/{catalog}/{schema}/{volume_name}/bpm_perf",
    "m2e_audit_logs_path": f"/Volumes/{catalog}/{schema}/{volume_name}/m2e_audit",
    "m2e_perf_logs_path": f"/Volumes/{catalog}/{schema}/{volume_name}/m2e_perf",
    "maf_perf_logs_path": f"/Volumes/{catalog}/{schema}/{volume_name}/maf_perf",
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

def load_bpm_adapter_template():
    """Load BPM adapter log template structure"""
    return {
        "context": "default",
        "filename": None,
        "level": "DEBUG",
        "log_uuid": None,
        "logger": "adapter.performance.tracking",
        "mdc": {
            "service.name": None,
            "span_id": None,
            "trace_flags": "00",
            "trace_id": None
        },
        "message": {
            "ConversationId": None,
            "ClientApp": None,
            "ClientDME2Lookup": "",
            "Mode": "",
            "Service": None,
            "CustomTag": None,
            "TrailMarks": [],
            "Cluster": "AE2P01",
            "Token": "",
            "Vtier": "N/A",
            "TransactionId": None,
            "InstanceName": None,
            "HostIPAddress": None,
            "ClientId": None,
            "StartTimeStamp": None,
            "Application": "DUCSDB Adapter",
            "ReqMsgSize": None,
            "RespMsgSize": 222,
            "HostName": None
        },
        "raw-message": None,
        "thread": None,
        "timestamp": None
    }

def load_bpm_audit_template():
    """Load BPM audit log template structure"""
    return {
        "contextMap": {},
        "endOfBatch": False,
        "filename": None,
        "level": "INFO",
        "log_uuid": None,
        "loggerFqcn": "org.apache.logging.slf4j.Log4jLogger",
        "loggerName": "audit",
        "message": {
            "Description": None,
            "InstanceName": None,
            "ApplicationId": None,
            "OriginalMessageId": None,
            "UniqueTransactionId": None,
            "OriginatorId": None,
            "Subject": None,
            "ConversationId": None,
            "OriginationSystemId": None,
            "OriginationSystemVersion": None,
            "OriginationSystemName": None,
            "SourceClass": "com.att.m2e.csi.CSIAuditor",
            "SourceMethod": None,
            "TransactionName": None,
            "TransactionStatus": "COMPLETE",
            "HostIPAddress": None,
            "HostName": None,
            "ResponseCode": "0",
            "ResponseDescription": None,
            "FaultTimestamp": None,
            "FaultSequenceNumber": None,
            "FaultLevel": None,
            "FaultCode": None,
            "FaultDescription": None,
            "ExternalFaultCode": None,
            "ExternalFaultDescription": None,
            "FaultEntity": None,
            "InitiatedTimestamp": None,
            "ElapsedTime": None,
            "Mode": "",
            "ServiceKeyData1": "",
            "ServiceKeyData2": "",
            "Cluster": "AE2P01",
            "ClientApp": None,
            "Vtier": "N/A",
            "ClientIP": None,
            "HttpMethod": None,
            "RequestURL": None
        },
        "thread": None,
        "threadId": None,
        "threadPriority": 5,
        "timeMillis": None
    }

def load_bpm_perf_template():
    """Load BPM performance log template structure - similar to BPM adapter"""
    return load_bpm_adapter_template()

def load_m2e_audit_template():
    """Load M2E audit log template structure - similar to BPM audit"""
    return load_bpm_audit_template()

def load_m2e_perf_template():
    """Load M2E performance log template structure"""
    return {
        "contextMap": {},
        "endOfBatch": False,
        "filename": None,
        "level": "DEBUG",
        "log_uuid": None,
        "loggerFqcn": "org.apache.logging.slf4j.Log4jLogger",
        "loggerName": "performance.tracking",
        "message": {
            "ConversationId": None,
            "ClientApp": None,
            "ClientDME2Lookup": None,
            "Mode": "",
            "Service": None,
            "CustomTag": None,
            "TrailMarks": [],
            "Cluster": "AE2P01",
            "Token": "",
            "Vtier": "N/A",
            "TransactionId": None,
            "InstanceName": None,
            "HostIPAddress": None,
            "ClientId": None,
            "StartTimeStamp": None,
            "Application": None,
            "ReqMsgSize": None,
            "RespMsgSize": None,
            "HostName": None,
            "ResponseCode": None,
            "ExternalFaultCode": None,
            "ExternalFaultDescription": None,
            "FaultEntity": None
        },
        "endOfBatch": False,
        "loggerFqcn": "org.apache.logging.slf4j.Log4jLogger",
        "threadId": None,
        "threadPriority": 5,
        "timeMillis": None
    }

def load_maf_perf_template():
    """Load MAF performance log template structure - similar to M2E performance"""
    return load_m2e_perf_template()

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
    
    def generate_bpm_adapter_log(self, inject_anomaly=False, anomaly_type=None):
        """Generate a single BPM adapter log entry"""
        log = load_bpm_adapter_template()
        now = datetime.utcnow()
        
        # Basic fields
        log["log_uuid"] = str(uuid.uuid4())
        log["timestamp"] = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+0000"
        log["thread"] = f"DispatchThread: [com.ibm.mq.jmqi.remote.impl.RemoteSession[:/{''.join(random.choices('0123456789abcdef', k=8))}]]"
        log["filename"] = f"/opt/log/{uuid.uuid4()}/volumes/kubernetes.io~empty-dir/bpmadapter-vol/adapter-perflogfile.log"
        
        # MDC fields
        service = random.choice(self.service_names)
        log["mdc"]["service.name"] = f"csbobpm-{service.replace(' ', '_')}"
        log["mdc"]["span_id"] = fake.sha1()[:16]
        log["mdc"]["trace_id"] = fake.sha1()
        
        # Message fields
        log["message"]["Service"] = service
        log["message"]["ConversationId"] = f"Enabler~CNG-CSI~{uuid.uuid4()}"
        log["message"]["ClientApp"] = f"M2E~adapter-{random.randint(1, 300)}-{random.randint(1, 10)}-{random.randint(1, 50)}-ae2p01~Resume"
        log["message"]["TransactionId"] = f"ServiceGateway{random.randint(100000, 999999)}@p102csg{random.randint(1,4)}c{random.randint(1,9)}_{uuid.uuid4()}"
        log["message"]["InstanceName"] = f"adapter-{random.randint(1, 300)}-{random.randint(1, 10)}-{random.randint(1, 50)}-ae2p01-{fake.slug()}"
        log["message"]["HostIPAddress"] = fake.ipv4_private()
        log["message"]["HostName"] = fake.hostname()
        log["message"]["CustomTag"] = random.choice(["Enabler", "BOBPM", "idp", "opus"])
        log["message"]["ClientId"] = log["message"]["CustomTag"]
        log["message"]["StartTimeStamp"] = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
        log["message"]["ReqMsgSize"] = random.randint(500, 5000)
        log["message"]["RespMsgSize"] = random.randint(100, 1000)
        
        # Trail marks
        if inject_anomaly and anomaly_type == "timeout":
            duration = random.randint(60000, 120000)
            complete = False
        elif inject_anomaly and anomaly_type == "high_response_time":
            duration = random.randint(35000, 90000)
            complete = True
        else:
            duration = random.randint(5, 50)
            complete = True
        
        start = now - timedelta(milliseconds=duration)
        log["message"]["TrailMarks"] = [
            {
                "Name": "AdapterHelper",
                "Detail": "executeOperation",
                "StartTime": start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z",
                "EndTime": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z" if complete else None,
                "Complete": complete,
                "ElapsedDuration": duration
            }
        ]
        
        log["raw-message"] = json.dumps(log["message"])
        return log
    
    def generate_bpm_audit_log(self, inject_anomaly=False, anomaly_type=None):
        """Generate a single BPM audit log entry"""
        log = load_bpm_audit_template()
        now = datetime.utcnow()
        
        # Basic fields
        log["timeMillis"] = int(now.timestamp() * 1000)
        log["thread"] = f"com.att.m2e.csi.CSIM2EBusLogic-Thread-{random.randint(100, 1000)}"
        log["threadId"] = random.randint(100, 1500)
        log["log_uuid"] = str(uuid.uuid4())
        log["filename"] = f"/opt/log/{uuid.uuid4()}/volumes/kubernetes.io~empty-dir/bpmaudit-vol/bpm-auditlogfile.log"
        
        # Add context map with tracing
        if random.random() > 0.5:
            log["contextMap"] = {
                "service.name": f"csbobpm-{random.choice(['BOBPM', 'unifiedservices', 'fobpm'])}",
                "span_id": fake.sha1()[:16],
                "trace_flags": random.choice(["00", "01"]),
                "trace_id": fake.sha1()
            }
        
        # Message fields
        transaction = random.choice(self.transaction_names)
        log["message"]["TransactionName"] = transaction
        log["message"]["ApplicationId"] = random.choice(["BOBPM", "opus", "isaac", "idpcustgraph"])
        log["message"]["InstanceName"] = f"bpm-service-{random.randint(1, 300)}-{random.randint(1, 10)}-{random.randint(1, 50)}-ae2p01-{fake.slug()}"
        log["message"]["UniqueTransactionId"] = f"ServiceGateway{random.randint(100000, 999999)}@p102csg{random.randint(1,4)}c{random.randint(1,9)}_{uuid.uuid4()}"
        log["message"]["ConversationId"] = f"{log['message']['ApplicationId']}~CNG-CSI~{uuid.uuid4()}"
        log["message"]["Subject"] = f"CW.pub.spm2.{transaction}.response"
        log["message"]["HostIPAddress"] = fake.ipv4_private()
        log["message"]["HostName"] = fake.hostname()
        log["message"]["OriginalMessageId"] = str(uuid.uuid4())
        log["message"]["ClientApp"] = f"ServiceGateway~{random.randint(100000, 999999)}@p102csg{random.randint(1,4)}c{random.randint(1,9)}~P11"
        
        # Timestamps
        initiated = now - timedelta(milliseconds=random.randint(1000, 5000))
        log["message"]["InitiatedTimestamp"] = initiated.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
        
        # Inject anomalies
        if inject_anomaly:
            if anomaly_type == "failure":
                log["message"]["ResponseCode"] = str(random.choice([500, 503, 504]))
                log["message"]["TransactionStatus"] = "FAILED"
                log["message"]["ElapsedTime"] = str(random.randint(5000, 15000))
            elif anomaly_type == "high_response_time":
                log["message"]["ElapsedTime"] = str(random.randint(35000, 90000))
            elif anomaly_type == "timeout":
                log["message"]["ResponseCode"] = "408"
                log["message"]["TransactionStatus"] = "TIMEOUT"
                log["message"]["ElapsedTime"] = str(random.randint(60000, 120000))
        else:
            log["message"]["ElapsedTime"] = str(random.randint(10, 3000))
        
        return log
    
    def generate_bpm_perf_log(self, inject_anomaly=False, anomaly_type=None):
        """Generate a single BPM performance log entry - similar to BPM adapter"""
        return self.generate_bpm_adapter_log(inject_anomaly, anomaly_type)
    
    def generate_m2e_audit_log(self, inject_anomaly=False, anomaly_type=None):
        """Generate a single M2E audit log entry - similar to BPM audit"""
        return self.generate_bpm_audit_log(inject_anomaly, anomaly_type)
    
    def generate_m2e_perf_log(self, inject_anomaly=False, anomaly_type=None):
        """Generate a single M2E performance log entry"""
        log = load_m2e_perf_template()
        now = datetime.utcnow()
        
        # Basic fields
        log["timeMillis"] = int(now.timestamp() * 1000)
        log["thread"] = f"com.att.adapter.hydra.ServiceAdapterHandler-Thread-{random.randint(1, 200)}"
        log["threadId"] = random.randint(100, 250)
        log["log_uuid"] = str(uuid.uuid4())
        log["filename"] = f"/opt/log/{uuid.uuid4()}/volumes/kubernetes.io~empty-dir/m2eperf-vol/m2e-perflogfile.log"
        
        # Context map
        log["contextMap"] = {
            "service.name": "csi-m2e-adapter",
            "span_id": fake.sha1()[:16],
            "trace_flags": "00",
            "trace_id": fake.sha1()
        }
        
        # Message fields
        service = random.choice(self.service_names)
        log["message"]["Service"] = service
        log["message"]["ConversationId"] = f"m2e~CNG-CSI~{uuid.uuid4()}"
        log["message"]["ClientApp"] = f"M2E~m2e-client-{random.randint(1, 300)}-{random.randint(1, 10)}-{random.randint(1, 50)}-ae2p01~Process"
        log["message"]["TransactionId"] = f"M2E{random.randint(1000000000, 9999999999)}"
        log["message"]["InstanceName"] = f"m2e-adapter-{random.randint(1, 300)}-{random.randint(1, 10)}-{random.randint(1, 50)}-ae2p01-{fake.slug()}"
        log["message"]["HostIPAddress"] = fake.ipv4_private()
        log["message"]["HostName"] = fake.hostname()
        log["message"]["CustomTag"] = random.choice(["m2e", "idp", "opus"])
        log["message"]["ClientId"] = log["message"]["CustomTag"]
        log["message"]["StartTimeStamp"] = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
        log["message"]["Application"] = "M2E Adapter"
        log["message"]["ReqMsgSize"] = random.randint(500, 3000)
        log["message"]["RespMsgSize"] = random.randint(100, 5000)
        
        # Trail marks with anomaly injection
        if inject_anomaly and anomaly_type == "timeout":
            duration = random.randint(60000, 120000)
            complete = False
        elif inject_anomaly and anomaly_type == "high_response_time":
            duration = random.randint(35000, 90000)
            complete = True
        else:
            duration = random.randint(3, 100)
            complete = True
        
        start = now - timedelta(milliseconds=duration)
        log["message"]["TrailMarks"] = [
            {
                "Name": "AdapterHelper",
                "Detail": "processRequest",
                "StartTime": start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z",
                "EndTime": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z" if complete else None,
                "Complete": complete,
                "ElapsedDuration": duration
            }
        ]
        
        if inject_anomaly and anomaly_type == "failure":
            log["message"]["ResponseCode"] = str(random.choice([500, 503, 504]))
            log["message"]["ExternalFaultCode"] = "1"
            log["message"]["ExternalFaultDescription"] = "M2E.processRequest.Error"
            log["message"]["FaultEntity"] = "M2E"
        
        return log
    
    def generate_maf_perf_log(self, inject_anomaly=False, anomaly_type=None):
        """Generate a single MAF performance log entry - similar to M2E performance"""
        return self.generate_m2e_perf_log(inject_anomaly, anomaly_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## File Writing Functions

# COMMAND ----------

def write_logs_to_volume(logs, log_type, config):
    """
    Write a batch of logs to the appropriate volume path using dbutils
    
    Args:
        logs: List of log dictionaries
        log_type: Type of logs ('audit', 'bpm', 'performance', 'bpm_adapter', 'bpm_audit', 'bpm_perf', 'm2e_audit', 'm2e_perf', 'maf_perf')
        config: Configuration dictionary
    """

    # Determine the path based on log type
    path_key = f"{log_type}_logs_path"
    if path_key not in config:
        raise ValueError(f"Unknown log type: {log_type}")
    
    path = config[path_key]
    
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
                ("bpm_adapter", generator.generate_bpm_adapter_log),
                ("bpm_audit", generator.generate_bpm_audit_log),
                ("bpm_perf", generator.generate_bpm_perf_log),
                ("m2e_audit", generator.generate_m2e_audit_log),
                ("m2e_perf", generator.generate_m2e_perf_log),
                ("maf_perf", generator.generate_maf_perf_log)
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

