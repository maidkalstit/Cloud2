from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
from src.config import config

def purge_anomalies():
    """
    Scans the existing Silver layer Iceberg table, relocates any legacy records
    with null or empty primary keys into silver_pending_review (DLQ), and removes
    them from silver_transactions to satisfy the strict Data Quality contract.
    """
    spark = (
        SparkSession.builder
        .appName("Olist-Lakehouse-SilverPurge")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,"
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.19"
        )
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.demo.type", "hadoop")
        .config("spark.sql.catalog.demo.warehouse", config.gcs_warehouse_path)
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .getOrCreate()
    )

    print("Checking for legacy anomalies in demo.silver.silver_transactions...")
    silver_df = spark.table("demo.silver.silver_transactions")
    
    anomalies_df = silver_df.filter(
        col("order_id").isNull() | 
        (col("order_id") == "") | 
        col("customer_id").isNull() | 
        (col("customer_id") == "") |
        col("created_date").isNull()
    )
    
    anomaly_count = anomalies_df.count()
    print(f"Found {anomaly_count} anomaly records violating Data Contracts.")
    
    if anomaly_count > 0:
        print("Moving anomalies to demo.silver.silver_pending_review...")
        # Prepare for DLQ pending review
        quarantine_df = anomalies_df.withColumn("_status", lit("PENDING"))
        quarantine_df.write.format("iceberg").mode("append").save("demo.silver.silver_pending_review")
        
        print("Purging anomalies from demo.silver.silver_transactions...")
        spark.sql("""
            DELETE FROM demo.silver.silver_transactions 
            WHERE order_id IS NULL OR order_id = '' OR customer_id IS NULL OR customer_id = '' OR created_date IS NULL
        """)
        print("Successfully cleaned Silver layer!")
    else:
        print("Silver layer is already 100% clean!")

if __name__ == "__main__":
    purge_anomalies()
