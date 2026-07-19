import os
import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Absolute import naming conventions strictly followed
from src.governance.remediation_rules import apply_auto_remediation

@pytest.fixture(scope="module")
def spark_test_session() -> SparkSession:
    """
    Initializes a lightweight, local SparkSession optimized for testing remediation rules.
    Configures Windows environment variables to prevent Python worker connection failures.
    
    Why: Reusing a single-threaded SparkSession across this test module prevents JVM 
    thrashing and safeguards our strict 4GB RAM testing constraints.
    """
    # Resolve potential Python interpreter path issues on Windows platforms
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("Olist-Remediation-UnitTests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )
    yield session
    session.stop()

def test_apply_auto_remediation_fixes_empty_and_null_values(spark_test_session: SparkSession) -> None:
    """
    Verifies that the 'FIX_EMPTY_STATUS' rule successfully corrects both empty strings 
    and NULL values to 'UNKNOWN', while leaving valid records untouched.
    
    Why: Ensures the metadata columns '_is_remediated' and '_remediation_rule' are 
    correctly mapped for full downstream auditability.
    """
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("order_status", StringType(), True)
    ])
    
    # 3 Scenarios to isolate:
    # Row 1: Valid clean status -> Should remain untouched, metadata must be False/None
    # Row 2: Empty status string "" -> Should be auto-fixed, metadata must capture the rule
    # Row 3: True NULL value -> Should be auto-fixed, metadata must capture the rule
    mock_records = [
        ("o1", "delivered"),
        ("o2", ""),
        ("o3", None)
    ]
    
    df = spark_test_session.createDataFrame(mock_records, schema)
    
    # Execute remediation pipeline
    remediated_df = apply_auto_remediation(df, target_column="order_status", rule_name="FIX_EMPTY_STATUS")
    results = remediated_df.orderBy("order_id").collect()
    
    assert len(results) == 3
    
    # 1. Assertions for Clean Row (o1)
    assert results[0]["order_status"] == "delivered", "Clean status was mutated incorrectly."
    assert results[0]["_is_remediated"] is False, "Clean row flagged as remediated."
    assert results[0]["_remediation_rule"] is None, "Clean row assigned a remediation rule name."
    
    # 2. Assertions for Empty String Row (o2)
    assert results[1]["order_status"] == "UNKNOWN", "Empty status string was not corrected to 'UNKNOWN'."
    assert results[1]["_is_remediated"] is True, "Remediated row failed to set '_is_remediated' to True."
    assert results[1]["_remediation_rule"] == "FIX_EMPTY_STATUS", "Metadata rule name mismatch."
    
    # 3. Assertions for NULL value Row (o3)
    assert results[2]["order_status"] == "UNKNOWN", "NULL status value was not corrected to 'UNKNOWN'."
    assert results[2]["_is_remediated"] is True, "NULL row failed to set '_is_remediated' to True."
    assert results[2]["_remediation_rule"] == "FIX_EMPTY_STATUS", "Metadata rule name mismatch."

def test_apply_auto_remediation_raises_value_error_on_unregistered_rule(spark_test_session: SparkSession) -> None:
    """
    Asserts that calling apply_auto_remediation with an unregistered rule name 
    immediately raises a ValueError.
    
    Why: Prevents silent failures where developers assume a rule was executed 
    but it was bypassed due to a registry configuration mismatch.
    """
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("order_status", StringType(), True)
    ])
    df = spark_test_session.createDataFrame([("o1", "delivered")], schema)
    
    # Assert fail-fast behavior
    with pytest.raises(ValueError) as exception_info:
        apply_auto_remediation(df, target_column="order_status", rule_name="NON_EXISTENT_RULE")
        
    assert "is not registered" in str(exception_info.value), "The execution did not fail-fast with a descriptive registry error."