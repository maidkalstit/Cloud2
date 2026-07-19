import os
import sys
import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, Row

# Absolute import naming conventions strictly followed
from src.governance.review import query_pending_transactions, apply_remediation_decision, run_governance_cli

@pytest.fixture(scope="module")
def spark_test_session() -> SparkSession:
    """
    Initializes a lightweight in-memory SparkSession for testing.
    Fixes Windows Python worker connection timeouts by forcing the exact .venv path.
    """
    # Fix JVM-to-Python connection issues on Windows machines
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("Olist-Governance-UnitTests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )
    yield session
    session.stop()

def test_query_pending_transactions_returns_filtered_rows(spark_test_session: SparkSession) -> None:
    """
    Asserts that query_pending_transactions retrieves only rows with 'PENDING' status.
    """
    # Mocking spark.table to intercept catalog reads without a live GCS Iceberg warehouse
    with patch.object(spark_test_session, "table") as mock_table:
        mock_df = MagicMock()
        mock_filtered_df = MagicMock()
        
        # Chain reaction: spark.table().filter().collect()
        mock_table.return_value = mock_df
        mock_df.filter.return_value = mock_filtered_df
        mock_filtered_df.collect.return_value = [
            Row(order_id="o1", _status="PENDING", payment_value="-100.00")
        ]
        
        results = query_pending_transactions(spark_test_session)
        
        # Verify the target table is correct and filter is applied to status column
        mock_table.assert_called_once_with("demo.silver.silver_pending_review")
        mock_df.filter.assert_called_once()
        assert len(results) == 1
        assert results[0]["order_id"] == "o1"

def test_apply_remediation_decision_approved_idempotency(spark_test_session: SparkSession) -> None:
    """
    Validates the APPROVED decision path. Crucially checks Idempotency:
    Compiles a MERGE INTO statement instead of append to prevent duplicate rows on re-runs.
    """
    mock_row = {
        "order_id": "o_idemp",
        "ingest_id": "i_idemp",
        "customer_id": "c1",
        "order_status": "delivered",
        "order_purchase_timestamp": "2026-01-01",
        "payment_value": "-150.00",
        "created_date": "2026-01-01"
    }
    
    with patch.object(spark_test_session, "sql") as mock_sql_engine, \
         patch("pyspark.sql.DataFrameWriter.save") as mock_audit_save:
         
        # Execute first approval run
        apply_remediation_decision(
            spark=spark_test_session,
            row=mock_row,
            decision="APPROVED",
            reviewer_name="TungDang_DE",
            remediated_value_str="150.00"
        )
        
        # Grab all compiled SQL statements executed during this run
        executed_queries = [call.args[0] for call in mock_sql_engine.call_args_list]
        
        # Idempotency Verification:
        # We assert that MERGE INTO is compiled, NOT a standard 'INSERT INTO' / 'APPEND' write.
        # MERGE INTO guarantees that running this exact approval twice yields exactly one row.
        assert any("MERGE INTO demo.silver.silver_transactions" in query for query in executed_queries)
        assert any("UPDATE demo.silver.silver_pending_review" in query for query in executed_queries)
        
        # Audit Verification: Ensure action is written to the centralized historical audit ledger
        mock_audit_save.assert_called_once()

def test_apply_remediation_decision_rejected(spark_test_session: SparkSession) -> None:
    """
    Validates the REJECTED path: No merge should trigger on Silver clean,
    and status must update to REJECTED in the pending catalog table.
    """
    mock_row = {
        "order_id": "o_rej",
        "ingest_id": "i_rej",
        "customer_id": "c2",
        "order_status": "shipped",
        "order_purchase_timestamp": "2026-01-02",
        "payment_value": "-50.00",
        "created_date": "2026-01-02"
    }
    
    with patch.object(spark_test_session, "sql") as mock_sql_engine, \
         patch("pyspark.sql.DataFrameWriter.save") as mock_audit_save:
         
        apply_remediation_decision(
            spark=spark_test_session,
            row=mock_row,
            decision="REJECTED",
            reviewer_name="TungDang_DE"
        )
        
        executed_queries = [call.args[0] for call in mock_sql_engine.call_args_list]
        
        # Rejected Row must never enter Silver Clean table (Security/Financial Integrity)
        assert not any("MERGE INTO demo.silver.silver_transactions" in query for query in executed_queries)
        # Status in review table must set to REJECTED for strict audit traces
        assert any("UPDATE demo.silver.silver_pending_review" in query for query in executed_queries)
        assert any("SET _status = 'REJECTED'" in query for query in executed_queries)
        
        # Central audit ledger must still record this administrative reject action
        mock_audit_save.assert_called_once()

def test_run_governance_cli_interactive_flow(spark_test_session: SparkSession) -> None:
    """
    Tests the end-to-end CLI wizard using mock injection for human inputs and console outputs.
    
    Why: Injecting mock functions for 'prompt_input' and 'printer' prevents the test suite 
    from hanging indefinitely, allowing automated testing of interactive systems.
    """
    # 1. Prepare mock pending rows to review
    pending_mock_rows = [
        Row(order_id="o_cli", ingest_id="i_cli", customer_id="c3", order_status="delivered", order_purchase_timestamp="2026-01-03", payment_value="-12.34", created_date="2026-01-03")
    ]
    
    # 2. Mock terminal input/output behaviors
    # We simulate a human typist typing 'Y' when prompted
    mock_input_feeder = MagicMock(return_value="Y")
    mock_console_printer = MagicMock()
    
    with patch("src.governance.review.query_pending_transactions", return_value=pending_mock_rows), \
         patch("src.governance.review.apply_remediation_decision") as mock_apply_decision:
         
         run_governance_cli(
             spark=spark_test_session,
             reviewer_name="TungDang_DE",
             prompt_input=mock_input_feeder,
             printer=mock_console_printer
         )
         
         # Assert CLI prompt was shown to the reviewer and they replied 'Y'
         mock_input_feeder.assert_called_once()
         
         # Assert the remediation handler was triggered with the absolute positive corrected value
         mock_apply_decision.assert_called_once_with(
             spark_test_session, 
             pending_mock_rows[0], 
             "APPROVED", 
             "TungDang_DE", 
             "12.34" # Absolute auto-correction value passed (Decimal string)
         )
         
         # Assert output notifications were written to stdout
         mock_console_printer.assert_any_call("-> Decision logged: APPROVED and remediated value to 12.34")