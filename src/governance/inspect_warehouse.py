import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

# Add project root directory to sys.path automatically
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Set PySpark Python path
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = os.path.abspath(os.path.join(PROJECT_ROOT, "hadoop"))

from src.config import config

def inspect_lakehouse():
    print("=" * 60)
    print("      ENTERPRISE LAKEHOUSE AUDIT & INSPECTION REPORT     ")
    print("=" * 60)

    spark = (
        SparkSession.builder
        .appName("Olist-Lakehouse-Inspector")
        .master("local[1]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,"
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.19"
        )
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", config.gcs_warehouse_path)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .getOrCreate()
    )

    # 1. Inspect Bronze Layer (Raw Ledger)
    try:
        bronze_count = spark.table("bronze.bronze_transactions").count()
        print(f"\n1. BRONZE LAYER (bronze.bronze_transactions):")
        print(f"   -> Total Raw Ingested Records : {bronze_count:,}")
        print("   -> Sample Data:")
        spark.table("bronze.bronze_transactions").select("ingest_id", "order_id", "order_status", "payment_value", "ingested_at").show(3, truncate=False)
    except Exception as e:
        print(f"   -> Bronze Table Error: {e}")

    # 2. Inspect Silver Clean Layer
    try:
        silver_clean_count = spark.table("silver.silver_transactions").count()
        print(f"\n2. SILVER CLEAN LAYER (silver.silver_transactions):")
        print(f"   -> Total Clean & Remediated Records: {silver_clean_count:,}")
        print("   -> Validation Status Breakdown:")
        spark.table("silver.silver_transactions").groupBy("validation_status", "_is_remediated").count().show()
        print("   -> Sample Clean Data:")
        spark.table("silver.silver_transactions").select("order_id", "parsed_payment_value", "validation_status", "_is_remediated", "_remediation_rule").show(3, truncate=False)
    except Exception as e:
        print(f"   -> Silver Clean Error: {e}")

    # 3. Inspect Silver Pending Review (Quarantine)
    try:
        silver_pending_count = spark.table("silver.silver_pending_review").count()
        print(f"\n3. SILVER QUARANTINE LAYER (silver.silver_pending_review):")
        print(f"   -> Total Quarantined Records : {silver_pending_count:,}")
        print("   -> Status Breakdown (_status):")
        spark.table("silver.silver_pending_review").groupBy("_status").count().show()
    except Exception as e:
        print(f"   -> Silver Pending Error: {e}")

    # 4. Inspect Governance Audit Log
    try:
        audit_count = spark.table("silver.audit_log").count()
        print(f"\n4. GOVERNANCE AUDIT LOG (silver.audit_log):")
        print(f"   -> Total Audited Decisions   : {audit_count:,}")
        if audit_count > 0:
            print("   -> Human Governance Decisions Log:")
            spark.table("silver.audit_log").show(truncate=False)
    except Exception as e:
        print(f"   -> Audit Log Info: {e}")

    # 5. Inspect Gold Layer (DBT Materializations)
    try:
        print("\n5. GOLD LAYER (gold_finance & gold_marketing):")
        
        # Finance: Daily Revenue
        revenue_count = spark.table("gold_finance.daily_revenue").count()
        print(f"   -> [Finance] Total Days Calculated: {revenue_count:,}")
        print("   -> Sample Daily Revenue Data:")
        spark.table("gold_finance.daily_revenue").orderBy(col("revenue_date").desc()).show(3, truncate=False)
        
        # Marketing: Customer Activity
        customer_count = spark.table("gold_marketing.customer_activity").count()
        print(f"   -> [Marketing] Total Unique Customers: {customer_count:,}")
        print("   -> Sample Customer Activity Data:")
        spark.table("gold_marketing.customer_activity").orderBy(col("total_spent").desc()).show(3, truncate=False)
        
    except Exception as e:
        print(f"   -> Gold Table Error (Did you run dbt yet?): {e}")

    print("\n" + "=" * 60)
    print("                 INSPECTION COMPLETE                         ")
    print("=" * 60)

if __name__ == "__main__":
    inspect_lakehouse()
