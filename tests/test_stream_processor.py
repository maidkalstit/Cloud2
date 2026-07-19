import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), "hadoop"))


# Absolute import patterns strictly applied
from src.stream_processor import deserialize_kafka_payload, execute_micro_batch_storage, get_base_bronze_schema

@pytest.fixture(scope="module")
def spark_test_session() -> SparkSession:
    """
    Initializes a lightweight, single-threaded in-memory SparkSession for isolated unit testing.
    
    Why: Spinning up a full Spark cluster infrastructure inside a test container takes minutes 
    and violates our 4GB RAM boundary. A local[1] master runs instantly inside the JVM memory space.
    """
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("Olist-Lakehouse-UnitTests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )
    yield session
    session.stop()

def test_deserialize_kafka_payload_transforms_binary_to_columns(spark_test_session: SparkSession) -> None:
    """
    Verifies that raw binary strings arriving from the Kafka broker are successfully unpacked 
    into structured Spark columns conforming to the base Bronze schema.
    """
    base_schema: StructType = get_base_bronze_schema()
    
    # Simulate the raw byte payload wrapper typically delivered by Spark's reactive Kafka source
    raw_kafka_mock_data = [(b'{"ingest_id": "i-123", "order_id": "o-456", "customer_id": "c-789", "order_status": "delivered", "order_purchase_timestamp": "2026-01-01 12:00:00", "payment_value": "99.50"}',)]
    raw_df = spark_test_session.createDataFrame(raw_kafka_mock_data, ["value"])
    
    # Execute extraction logic
    unpacked_df = deserialize_kafka_payload(raw_df, base_schema)
    assert unpacked_df.schema == base_schema
    
    collected_result = unpacked_df.collect()[0]
    assert collected_result["ingest_id"] == "i-123"
    assert collected_result["order_id"] == "o-456"
    assert collected_result["payment_value"] == "99.50"

@patch("src.stream_processor.config")
def test_execute_micro_batch_storage_routes_and_remediates_correctly(
    mock_config: MagicMock, 
    spark_test_session: SparkSession
) -> None:
    """
    Validates DLQ Tier 2 multiplexing: Assures clean data routes to Silver Clean, 
    empty statuses trigger auto-remediation rules, and negative balances route to Pending Review.
    """
    base_schema: StructType = get_base_bronze_schema()
    
    # Prepare a heterogeneous batch: Row 1 (Clean), Row 2 (Tier 2a Remediation Target), Row 3 (Tier 2b DLQ Target)
    heterogeneous_mock_rows = [
        ("i_clean", "o_clean", "c1", "delivered", "2026-01-01 10:00:00", "150.00"),
        ("i_remed", "o_remed", "c2", "", "2026-01-02 11:00:00", "50.25"),
        ("i_pending", "o_pending", "c3", "shipped", "2026-01-03 12:00:00", "-250.00")
    ]
    batch_df = spark_test_session.createDataFrame(heterogeneous_mock_rows, base_schema)

    # Intercept Spark SQL calls to bypass the lack of a real cloud Iceberg warehouse catalog during tests
    with patch.object(spark_test_session, "sql") as mock_sql_engine, \
         patch("pyspark.sql.DataFrameWriter.save") as mock_bronze_save:
         
        execute_micro_batch_storage(batch_df, batch_id=42)
        
        # 1. Assert Bronze Layer absolute retention path
        mock_bronze_save.assert_called_once()
        
        # 2. View Interception: Query the temporary staging views registered inside the active Spark Catalog
        # This isolates and verifies the filtering/remediation data state right before the SQL MERGE triggers.
        silver_clean_staging_df = spark_test_session.table("tmp_batch_silver_clean")
        pending_review_staging_df = spark_test_session.table("tmp_batch_pending")
        
        clean_rows = silver_clean_staging_df.collect()
        pending_rows = pending_review_staging_df.collect()
        
        # 3. Assertions for Silver Clean Table & Remediation Logic (Row 1 & Row 2)
        assert len(clean_rows) == 2
        
        # Verify the auto-remediated row properties
        remediated_row = next(r for r in clean_rows if r["order_id"] == "o_remed")
        assert remediated_row["order_status"] == "UNKNOWN", "Empty status should be inline auto-corrected to 'UNKNOWN'."
        assert remediated_row["_is_remediated"] is True
        assert remediated_row["_remediation_rule"] == "FIX_EMPTY_STATUS"
        
        # 4. Assertions for Silver Pending Review Table / DLQ Tier 2b (Row 3)
        assert len(pending_rows) == 1
        assert pending_rows[0]["order_id"] == "o_pending"
        assert pending_rows[0]["_status"] == "PENDING"
        
        # 5. Verify that Iceberg MERGE commands were successfully compiled and submitted
        assert mock_sql_engine.call_count == 2
        executed_sql_queries = [call.args[0] for call in mock_sql_engine.call_args_list]
        assert any("MERGE INTO demo.silver.silver_transactions" in query for query in executed_sql_queries)
        assert any("MERGE INTO demo.silver.silver_pending_review" in query for query in executed_sql_queries)