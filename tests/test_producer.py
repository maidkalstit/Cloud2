import pytest
import random
from typing import Dict, Any
from unittest.mock import MagicMock

from src.config import config
from src.producer import inject_fault_payload, process_and_send_event, stream_csv_reader

def test_stream_csv_reader_generator_behavior(tmp_path: pytest.TempPathFactory) -> None:
    """Verifies that stream_csv_reader returns a real Python generator to safeguard memory limits."""
    csv_file = tmp_path / "mock_olist_transactions.csv"
    # Added order_id column to mock CSV data source
    csv_file.write_text(
        "order_id,customer_id,order_status,order_purchase_timestamp,payment_value\n"
        "o1,c1,delivered,2026-01-01 10:00:00,99.90\n"
    )
    
    generator = stream_csv_reader(str(csv_file))
    assert hasattr(generator, "__next__")
    first_row = next(generator)
    assert first_row["order_id"] == "o1"
    assert first_row["payment_value"] == "99.90"

def test_inject_fault_payload_deterministic_seeded_rates() -> None:
    """Uses a fixed random seed to accurately validate independent error injection counts."""
    random.seed(42)
    # Fixed: Fully populated clean payload containing stable base identities
    clean_payload: Dict[str, Any] = {
        "ingest_id": "ingest_123",
        "order_id": "order_123", 
        "customer_id": "cust_123",
        "order_status": "delivered",
        "order_purchase_timestamp": "2026-01-02 11:00:00",
        "payment_value": "150.00"
    }
    
    structural_anomalies: int = 0
    business_anomalies: int = 0
    test_iterations: int = 1000
    
    for _ in range(test_iterations):
        mutated_data = inject_fault_payload(clean_payload, structural_rate=0.10, business_rate=0.20)
        
        if mutated_data.get("order_id") is None:
            structural_anomalies += 1
        elif mutated_data.get("order_status") == "" or mutated_data.get("payment_value") == "-150.75":
            business_anomalies += 1

    # Tightened mathematical expectations based on seed=42 distribution analysis
    # Expected structural: ~100 rows, Expected business: ~200 rows out of 1000 iterations
    assert 80 <= structural_anomalies <= 120, f"Structural anomalies count ({structural_anomalies}) drifted out of expected tolerance."
    assert 180 <= business_anomalies <= 220, f"Business anomalies count ({business_anomalies}) drifted out of expected tolerance."

def test_process_and_send_event_happy_path_success() -> None:
    """Asserts correct routing and payload generation when data fully complies with the Avro contract."""
    mock_kafka_producer = MagicMock()
    mock_avro_serializer = MagicMock(return_value=b"\x00\x01_serialized_avro_binary")
    mock_raw_serializer = MagicMock()
    mock_alert_sender = MagicMock()
    
    sample_raw_row = {
        "order_id": "o_clean",
        "customer_id": "c_clean", 
        "order_status": "shipped", 
        "order_purchase_timestamp": "2026-01-03", 
        "payment_value": "20.00"
    }
    
    config.INJECT_ERROR = False
    
    process_and_send_event(
        raw_row=sample_raw_row,
        kafka_producer=mock_kafka_producer,
        avro_serializer=mock_avro_serializer,
        raw_serializer=mock_raw_serializer,
        alert_sender=mock_alert_sender
    )
    
    mock_avro_serializer.assert_called_once()
    mock_kafka_producer.send.assert_called_once_with(
        topic=config.KAFKA_TOPIC_RAW_TRANSACTIONS,
        value=b"\x00\x01_serialized_avro_binary"
    )

def test_process_and_send_event_routes_to_dlq_tier1_on_serialization_failure() -> None:
    """Validates DLQ Tier 1 operational path routing on forced Avro core validation exception."""
    mock_kafka_producer = MagicMock()
    mock_avro_serializer = MagicMock(side_effect=TypeError("Avro serialization type mismatch error"))
    mock_raw_serializer = MagicMock(return_value=b'{"fallback": "json_raw"}')
    mock_alert_sender = MagicMock()
    
    sample_corrupted_row = {
        "order_id": "o_corrupt",
        "customer_id": "c_corrupt", 
        "order_status": "delivered", 
        "order_purchase_timestamp": "2026-01-04", 
        "payment_value": "45.00"
    }
    
    process_and_send_event(
        raw_row=sample_corrupted_row,
        kafka_producer=mock_kafka_producer,
        avro_serializer=mock_avro_serializer,
        raw_serializer=mock_raw_serializer,
        alert_sender=mock_alert_sender
    )
    
    mock_avro_serializer.assert_called_once()
    mock_kafka_producer.send.assert_called_once_with(
        topic=config.KAFKA_TOPIC_DLQ_STRUCTURAL,
        value=b'{"fallback": "json_raw"}'
    )
    mock_alert_sender.assert_called_once()