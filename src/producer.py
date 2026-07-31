import csv
import time
import random
from datetime import datetime
from typing import Generator, Dict, Any, Callable
from uuid6 import uuid7

from src.config import config

# --- Avro Schema Definition (Updated to include technical ingest_id) ---
AVRO_SCHEMA_STR: str = """
{
  "type": "record",
  "name": "OrderTransaction",
  "namespace": "com.olist.lakehouse",
  "fields": [
    {"name": "ingest_id", "type": "string"},
    {"name": "order_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "order_status", "type": "string"},
    {"name": "order_purchase_timestamp", "type": "string"},
    {"name": "payment_value", "type": "string"}
  ]
}
"""

def stream_csv_reader(file_path: str) -> Generator[Dict[str, str], None, None]:
    """Streams a large CSV file line-by-line using a Python generator to maintain O(1) memory."""
    with open(file_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row

def inject_fault_payload(record: Dict[str, Any], structural_rate: float, business_rate: float) -> Dict[str, Any]:
    """Intentionally injects structural or business anomalies into clean records based on independent rates."""
    corrupted_record = record.copy()
    dice = random.random()

    # Tier 1: Structural Invalidation (Forces Avro Serialization Failure by nullifying a required field)
    if dice < structural_rate:
        corrupted_record["order_id"] = None  
        return corrupted_record

    # Tier 2: Business-Logic Invalidation (Passes Avro, Fails Downstream Rules)
    if dice >= structural_rate and dice < (structural_rate + business_rate):
        if random.choice([True, False]):
            corrupted_record["order_status"] = ""
        else:
            corrupted_record["payment_value"] = "-150.75"
            
    return corrupted_record

def process_and_send_event(
    raw_row: Dict[str, str],
    kafka_producer: Any,
    avro_serializer: Callable[[Dict[str, Any]], bytes],
    raw_serializer: Callable[[Dict[str, Any]], bytes],
    alert_sender: Callable[[str], None]
) -> None:
    """Orchestrates the lifecycle of a single streaming event with dual identity mapping."""
    # Split technical identity (ingest_id) from business identity (order_id)
    transaction_data: Dict[str, Any] = {
        "ingest_id": str(uuid7()), 
        "order_id": raw_row.get("order_id", ""), 
        "customer_id": raw_row.get("customer_id", ""),
        "order_status": raw_row.get("order_status", ""),
        "order_purchase_timestamp": raw_row.get("order_purchase_timestamp", ""),
        "payment_value": raw_row.get("payment_value", "0.00")
    }

    if config.INJECT_ERROR:
        transaction_data = inject_fault_payload(
            transaction_data, 
            config.STRUCTURAL_ERROR_RATE, 
            config.BUSINESS_ERROR_RATE
        )

    try:
        serialized_payload = avro_serializer(transaction_data)
        kafka_producer.produce(topic=config.KAFKA_TOPIC_RAW_TRANSACTIONS, value=serialized_payload)
    except Exception as serialization_error:
        fallback_payload = raw_serializer(transaction_data)
        kafka_producer.produce(topic=config.KAFKA_TOPIC_DLQ_STRUCTURAL, value=fallback_payload)

        
        alert_msg = f"ALERT: DLQ Tier 1 Triggered. Avro Serialization Failed: {str(serialization_error)}"
        alert_sender(alert_msg)

def send_slack_notification(message: str) -> None:
    """Sends an out-of-band monitoring alert to Slack webhook if configured."""
    webhook_url = None
    if config.SLACK_WEBHOOK_URL:
        webhook_url = config.SLACK_WEBHOOK_URL.get_secret_value()
        
    if not webhook_url:
        print(f"[Slack Alert Simulation] {message}")
        return
        
    try:
        import requests
        payload = {"text": message}
        response = requests.post(webhook_url, json=payload, timeout=5.0)
        if response.status_code != 200:
            print(f"Failed to send Slack alert (Status {response.status_code}): {response.text}")
    except Exception as e:
        print(f"Exception sending Slack alert: {str(e)}")

def get_merged_sorted_transactions(orders_path: str, payments_path: str) -> list:
    """Pre-loads orders and payments datasets, merges them by order_id, and sorts them chronologically."""
    payments = {}
    with open(payments_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            order_id = row["order_id"]
            try:
                val = float(row["payment_value"])
            except ValueError:
                val = 0.0
            payments[order_id] = payments.get(order_id, 0.0) + val

    transactions = []
    with open(orders_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            order_id = row["order_id"]
            payment_val = f"{payments.get(order_id, 0.0):.2f}"
            transactions.append({
                "order_id": order_id,
                "customer_id": row["customer_id"],
                "order_status": row["order_status"],
                "order_purchase_timestamp": row["order_purchase_timestamp"],
                "payment_value": payment_val
            })

    # Sort chronologically by order_purchase_timestamp
    def parse_dt(dt_str: str) -> datetime:
        try:
            return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.min

    transactions.sort(key=lambda x: parse_dt(x["order_purchase_timestamp"]))
    return transactions

def replay_transactions(
    transactions: list,
    speed_multiplier: float,
    send_fn: Callable[[Dict[str, str]], None]
) -> None:
    """Simulates real-world data streaming by sleeping relative to the chronological time between order timestamps."""
    prev_time = None
    for tx in transactions:
        dt_str = tx["order_purchase_timestamp"]
        try:
            curr_time = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            curr_time = None

        if prev_time and curr_time:
            time_diff = (curr_time - prev_time).total_seconds()
            if time_diff > 0:
                sleep_time = time_diff / speed_multiplier
                # Cap the sleep time to prevent extremely long delays during simulation gaps
                sleep_time = min(sleep_time, 1.0)
                time.sleep(sleep_time)

        send_fn(tx)
        if curr_time:
            prev_time = curr_time

if __name__ == "__main__":
    import os
    import json
    import io

    from confluent_kafka import Producer

    from fastavro import schemaless_writer, parse_schema

    print("Starting real-time Olist event simulation producer...")

    # Initialize Confluent Kafka Producer
    conf = {'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS}
    producer = Producer(conf)

    parsed_avro = parse_schema(json.loads(AVRO_SCHEMA_STR))

    def avro_serializer(data: Dict[str, Any]) -> bytes:
        bytes_io = io.BytesIO()
        schemaless_writer(bytes_io, parsed_avro, data)
        return bytes_io.getvalue()

    def raw_serializer(data: Dict[str, Any]) -> bytes:
        return json.dumps(data).encode('utf-8')

    def alert_sender(msg: str) -> None:
        send_slack_notification(msg)

    def send_fn(raw_row: Dict[str, str]) -> None:
        process_and_send_event(
            raw_row=raw_row,
            kafka_producer=producer,
            avro_serializer=avro_serializer,
            raw_serializer=raw_serializer,
            alert_sender=alert_sender
        )
        producer.poll(0)

    # Locate CSV data files
    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
    orders_file = os.path.join(data_dir, "olist_orders_dataset.csv")
    payments_file = os.path.join(data_dir, "olist_order_payments_dataset.csv")

    if not os.path.exists(orders_file) or not os.path.exists(payments_file):
        print(f"ERROR: Dataset files missing at {data_dir}")
        exit(1)

    print("Pre-loading and sorting datasets (this may take a few seconds)...")
    transactions = get_merged_sorted_transactions(orders_file, payments_file)
    print(f"Loaded {len(transactions)} sorted events. Replaying with speed factor {config.KAFKA_SPEED_MULTIPLIER}...")

    try:
        replay_transactions(transactions, config.KAFKA_SPEED_MULTIPLIER, send_fn)
    except KeyboardInterrupt:
        print("Replay simulation paused by user.")
    finally:
        producer.flush()
        print("Producer shutdown cleanly.")
