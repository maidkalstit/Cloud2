from decimal import Decimal
from datetime import datetime
from typing import Callable, List, Dict, Any
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col

# Absolute import naming convention strictly followed
from src.config import config

def query_pending_transactions(spark: SparkSession) -> List[Row]:
    """
    Queries the Iceberg 'silver_pending_review' table specifically for records marked as 'PENDING'.
    
    Why: Isolating pending records ensures the human auditor only reviews unprocessed, 
    quarantined business anomalies, preserving computing resources.
    """
    try:
        pending_df = (
            spark.table("demo.silver.silver_pending_review")
            .filter(col("_status") == "PENDING")
        )
        return pending_df.collect()
    except Exception as e:
        # If the table does not exist yet (first-run scenario), return an empty list gracefully
        return []

def apply_remediation_decision(
    spark: SparkSession,
    row: Row,
    decision: str,
    reviewer_name: str,
    remediated_value_str: str = "0.00"
) -> None:
    """
    Executes the database transaction for a single review action.
    On APPROVED (Y): Merges the corrected record into Silver Clean and logs to Audit.
    On REJECTED (N): Updates pending status and logs to Audit.
    
    Why: Uses Iceberg MERGE INTO and append operations to guarantee idempotency and auditability.
    """
    current_time = datetime.now().isoformat()
    order_id = row["order_id"]
    ingest_id = row["ingest_id"]
    old_value = row["payment_value"]

    # 1. Initialize empty tables representation for Spark execution
    # Ensure money fields utilize DECIMAL(18,2) representation at SQL compile time


    if decision == "APPROVED":
        remediated_decimal = Decimal(remediated_value_str)
        # Build the repaired record to match the Silver Clean schema exactly
        # Cast the string payload back to Decimal for target storage compatibility
        repaired_data = [{
            "ingest_id": ingest_id,
            "order_id": order_id,
            "customer_id": row["customer_id"],
            "order_status": row["order_status"],
            "order_purchase_timestamp": row["order_purchase_timestamp"],
            "payment_value": remediated_decimal,
            "created_date": row["created_date"],
            "_is_remediated": True,
            "_remediation_rule": "HUMAN_OVERRIDE"
        }]
        
        repaired_df = spark.createDataFrame(repaired_data)
        repaired_df.createOrReplaceTempView("tmp_human_repaired")
        
        # Merge corrected record into Silver Clean table
        spark.sql("""
            MERGE INTO demo.silver.silver_transactions target
            USING tmp_human_repaired source
            ON target.order_id = source.order_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        # Update the pending registry status to APPROVED
        spark.sql("""
            UPDATE demo.silver.silver_pending_review
            SET _status = 'APPROVED'
            WHERE order_id = :order_id AND ingest_id = :ingest_id
        """, args={"order_id": order_id, "ingest_id": ingest_id})
        
        action_type = "APPROVE_AND_FIX"
        new_value = str(remediated_decimal)

    else:  # REJECTED path
        # Mark as REJECTED in the pending registry so it remains for historical trace but closes the action
        spark.sql("""
            UPDATE demo.silver.silver_pending_review
            SET _status = 'REJECTED'
            WHERE order_id = :order_id AND ingest_id = :ingest_id
        """, args={"order_id": order_id, "ingest_id": ingest_id})
        action_type = "REJECT"
        new_value = "N/A"

    # 2. Append to centralized audit_log table for strict governance compliance
    audit_entry = [{
        "old_value": str(old_value),
        "new_value": new_value,
        "field_changed": "payment_value",
        "action": action_type,
        "reviewed_by": reviewer_name,
        "reviewed_at": current_time,
        "order_id": order_id
    }]
    
    audit_df = spark.createDataFrame(audit_entry)
    audit_df.write.format("iceberg").mode("append").save("demo.silver.audit_log")

def run_governance_cli(
    spark: SparkSession,
    reviewer_name: str,
    prompt_input: Callable[[str], str] = input,
    printer: Callable[[str], None] = print
) -> None:
    """
    Launches the interactive CLI interface allowing human auditors to step through quarantined transactions.
    
    Why: Injecting 'prompt_input' and 'printer' allows 100% automated test coverage without 
    blocking standard I/O streams during testing.
    """
    pending_records = query_pending_transactions(spark)
    
    if not pending_records:
        printer("No pending transactions found for review.")
        return

    printer(f"--- Found {len(pending_records)} Transactions Pending Review ---")
    
    for row in pending_records:
        printer("\n==================================================")
        printer(f"Order ID       : {row['order_id']}")
        printer(f"Ingest ID      : {row['ingest_id']}")
        printer(f"Payment Value  : {row['payment_value']} (ANOMALY: Negative Value)")
        printer(f"Status         : {row['order_status']}")
        printer("==================================================")
        
        decision = ""
        while decision not in ["Y", "N"]:
            decision = prompt_input("Approve and auto-fix to absolute value? (Y/N): ").strip().upper()
        
        if decision == "Y":
            # Auto-remediation for negative value: take the absolute positive amount
            fixed_val = str(abs(Decimal(str(row["payment_value"]))))
            apply_remediation_decision(spark, row, "APPROVED", reviewer_name, fixed_val)
            printer(f"-> Decision logged: APPROVED and remediated value to {fixed_val}")
        else:
            apply_remediation_decision(spark, row, "REJECTED", reviewer_name)
            printer("-> Decision logged: REJECTED (Record marked as inactive)")
            
    printer("\n--- Governance Review Session Completed ---")

if __name__ == "__main__":
    import os
    import sys
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["HADOOP_HOME"] = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "hadoop"))
    spark_session = (

        SparkSession.builder
        .appName("Olist-Governance-CLI-Main")
        .master("local[1]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,"
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.19"
        )


        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.demo.type", "hadoop")
        .config("spark.sql.catalog.demo.warehouse", config.gcs_warehouse_path)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .getOrCreate()
    )
    run_governance_cli(spark_session, reviewer_name="System Auditor")