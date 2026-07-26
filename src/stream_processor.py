from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, to_date, when, udf
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import StructType, StructField, StringType
import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), "hadoop"))
from decimal import Decimal
from pydantic import BaseModel, field_validator, ValidationError


from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

from src.governance.remediation_rules import apply_auto_remediation
# Enforce absolute import conventions
from src.config import config

# --- Pydantic Schema Definition for Business Rules Validation ---
class BusinessTransaction(BaseModel):
    ingest_id: str
    order_id: str
    customer_id: str
    order_status: str
    order_purchase_timestamp: str
    payment_value: Decimal

    @field_validator("order_status")
    @classmethod
    def check_status_not_empty(cls, v: str) -> str:
        if not v or v.strip() == "":
            raise ValueError("EMPTY_STATUS")
        return v

    @field_validator("payment_value")
    @classmethod
    def check_financial_sanity(cls, v: Decimal) -> Decimal:
        if v < 0:
            raise ValueError("NEGATIVE_VALUE")
        return v

def validate_row_logic(
    ingest_id: str, 
    order_id: str, 
    customer_id: str, 
    order_status: str, 
    order_purchase_timestamp: str, 
    payment_value: str
) -> str:
    """Validates an incoming record against the strict Pydantic schema rules."""
    try:
        try:
            parsed_val = Decimal(str(payment_value)) if payment_value else Decimal("0.00")
        except Exception:
            parsed_val = Decimal("0.00")

        BusinessTransaction(
            ingest_id=ingest_id or "",
            order_id=order_id or "",
            customer_id=customer_id or "",
            order_status=order_status or "",
            order_purchase_timestamp=order_purchase_timestamp or "",
            payment_value=parsed_val
        )
        return "VALID"

    except ValidationError as val_err:
        err_msg = str(val_err)
        if "EMPTY_STATUS" in err_msg:
            return "EMPTY_STATUS"
        if "NEGATIVE_VALUE" in err_msg:
            return "NEGATIVE_VALUE"
        return "INVALID"
    except Exception:
        return "INVALID"

validate_pydantic_udf = udf(validate_row_logic, StringType())

# --- Great Expectations Verification Wrapper ---
def validate_with_great_expectations(df: DataFrame) -> None:
    """Applies statistical quality validations to prevent downstream reporting corruptions."""
    if df.isEmpty():
        return

    ge_df = SparkDFDataset(df)
    res_order_id = ge_df.expect_column_values_to_not_be_null("order_id")
    res_payment = ge_df.expect_column_values_to_be_between("payment_value", min_value=0.0)

    if not (res_order_id.success and res_payment.success):
        from src.producer import send_slack_notification
        alert_msg = (
            f"WARNING: Great Expectations data validation check failed! "
            f"order_id_not_null success: {res_order_id.success}, "
            f"payment_value_positive success: {res_payment.success}."
        )
        send_slack_notification(alert_msg)

def init_spark_session() -> SparkSession:
    """
    Initializes a highly-tuned SparkSession tailored for a strict 4GB RAM hardware budget.
    Configures the Apache Iceberg catalog extension and local-to-GCS plumbing.
    
    Why: Default Spark configurations allocate loose memory buffers and spawn 200 default 
    shuffle partitions, which instantly triggers an Out-Of-Memory (OOM) crash on an e2-medium VM.
    """
    return (
        SparkSession.builder
        .appName("Olist-Lakehouse-StreamProcessor")
        # --- Critical 4GB RAM VM Tuning Parameters ---
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")  # Forces small parallel tasks, avoiding thread overhead

        .config("spark.default.parallelism", "2")
        .config("spark.memory.fraction", "0.6")       # Balanced execution vs storage memory split
        
        # --- Package Integrations (Kafka, Iceberg, Avro, and GCS) ---
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.3,"
            "org.apache.spark:spark-avro_2.13:4.0.3,"
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,"
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.19"
        )

        
        # --- Apache Iceberg Extensions & Catalog Configuration ---
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.demo.type", "hadoop")
        .config("spark.sql.catalog.demo.warehouse", config.gcs_warehouse_path)
        
        # --- GCS Connector Security Plugs ---
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .getOrCreate()
    )

def build_kafka_read_stream(spark: SparkSession) -> DataFrame:
    """
    Constructs an active streaming DataFrame attached to the primary inbound Kafka broker.
    Applies strict production-grade rate limiting parameters.
    
    Why: maxOffsetsPerTrigger acts as a backpressure safety valve, preventing large upstream 
    backlogs from inundating the compute engine during peak traffic hours.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC_RAW_TRANSACTIONS)
        .option("startingOffsets", "earliest")
        # Backpressure boundary regulation
        .option("maxOffsetsPerTrigger", str(config.SPARK_MAX_OFFSETS_PER_TRIGGER))
        .load()
    )

# --- Avro Schema (Must match Producer's schema exactly) ---
# Why: Avro is a binary, schema-dependent format. Both the writer (Producer) and 
# reader (Stream Processor) MUST share the exact same schema to correctly 
# encode/decode fields. Unlike JSON where field names are embedded in each record,
# Avro omits field names from the binary payload to save space — it relies entirely
# on the schema to know which bytes correspond to which field.
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

def deserialize_kafka_payload(raw_kafka_df: DataFrame) -> DataFrame:
    """
    Transforms raw binary Kafka records into a structured, typed DataFrame
    using Apache Avro deserialization.
    
    Why: The Producer serializes each record using fastavro's schemaless_writer(),
    which outputs compact Avro binary bytes. These bytes CANNOT be interpreted as
    UTF-8 text (JSON). Spark's from_avro() function understands the Avro binary
    format and uses the provided schema to correctly decode each field.
    
    Binary Layout Example (Avro vs JSON):
      JSON:  {"order_id": "abc"}     → 20 bytes (human-readable, field names included)
      Avro:  [0x06 0x61 0x62 0x63]   → 4 bytes  (compact, field names omitted)
             ^^^^                      ^^^^^^^^
             length=3 (varint)         raw UTF-8 "abc"
    """
    return (
        raw_kafka_df
        .select(from_avro(col("value"), AVRO_SCHEMA_STR).alias("data"))
        .select("data.*")
    )

def execute_micro_batch_storage(batch_df: DataFrame, batch_id: int) -> None:
    """
    Orchestrates the transactional write-paths for a single streaming micro-batch.
    Splits records into Bronze historical tracking, Silver Clean, and Silver Pending Review (DLQ Tier 2).
    
    guarantees that if a micro-batch fails halfway and retries, no duplicate records will enter Silver.
    """
    spark_session: SparkSession = batch_df.sparkSession
    if batch_df.isEmpty():
        return

    # 1. Enrich with Ingestion Metadata for Bronze Layer



    # Why: Bronze must serve as an absolute historical ledger of raw arrivals.
    bronze_enriched_df = batch_df.withColumn("ingested_at", current_timestamp())
    bronze_enriched_df.write.format("iceberg").mode("append").save("demo.bronze.bronze_transactions")

    # 2. Add structural date partitioning column & Money Precision Enforcement
    # Why: Casting to DECIMAL(18,2) prevents floating-point accounting drift.
    base_silver_df = (
        batch_df
        .withColumn("parsed_payment_value", col("payment_value").cast("decimal(18,2)"))
        .withColumn("created_date", to_date(col("order_purchase_timestamp")))
        .withColumn(
            "validation_status",
            validate_pydantic_udf(
                col("ingest_id"),
                col("order_id"),
                col("customer_id"),
                col("order_status"),
                col("order_purchase_timestamp"),
                col("payment_value")
            )
        )
    )

    # 3. Routing Branch B (DLQ Tier 2b - Ambiguous/Financial Errors to Pending Review)
    # Target: Negative monetary values or invalid Pydantic schemas.
    pending_review_filter = (col("validation_status") == "NEGATIVE_VALUE") | (col("validation_status") == "INVALID")
    pending_review_df = (
        base_silver_df
        .filter(pending_review_filter)
        .withColumn("_status", lit("PENDING"))
    )

    # 4. Routing Branch A (DLQ Tier 2a - Auto-remediation + Clean Data)
    # Target: Valid or simple remediation candidates (like empty status strings).
    good_pipeline_df = base_silver_df.filter(
        (col("validation_status") == "VALID") | (col("validation_status") == "EMPTY_STATUS")
    )
    remediated_clean_df = apply_auto_remediation(
        good_pipeline_df, 
        target_column="order_status", 
        rule_name="FIX_EMPTY_STATUS"
    )

    # 5. Deduplication Protection (Rule 2)
    # Why: Removes duplicate transaction keys in the micro-batch.
    deduplicated_clean_df = remediated_clean_df.dropDuplicates(["order_id"])

    # 6. Great Expectations Statistical Check
    # Verify dataset quality metrics before loading
    validate_with_great_expectations(deduplicated_clean_df)

    # 7. Idempotent Write via Iceberg MERGE INTO SQL Engine
    # Why: Prevents duplication across retries by updating records on matching Business Key (order_id)
    pending_review_df.createOrReplaceTempView("tmp_batch_pending")
    spark_session.sql("""
        MERGE INTO demo.silver.silver_pending_review target
        USING tmp_batch_pending source
        ON target.order_id = source.order_id AND target.ingest_id = source.ingest_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    deduplicated_clean_df.createOrReplaceTempView("tmp_batch_silver_clean")
    spark_session.sql("""
        MERGE INTO demo.silver.silver_transactions target
        USING tmp_batch_silver_clean source
        ON target.order_id = source.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

def start_streaming_job(spark: SparkSession, raw_stream_df: DataFrame) -> None:
    """
    Triggers the active execution loop of the PySpark Structured Streaming pipeline.
    Offsets and transactional progress are safely anchored onto GCS storage.
    
    Why: Hardcoding checkpoint location on local disk causes instant checkpoint loss 
    if the VM instance restarts. GCS provides persistent, multi-node durability.
    """
    structured_df = deserialize_kafka_payload(raw_stream_df)
    
    query = (
        structured_df.writeStream
        .foreachBatch(execute_micro_batch_storage)
        .option("checkpointLocation", config.gcs_checkpoint_path)
        .trigger(processingTime=config.SPARK_TRIGGER_PROCESSING_TIME)
        .start()
    )
    query.awaitTermination()
    
def init_iceberg_tables(spark: SparkSession) -> None:
    """Ensures Iceberg namespaces and tables are initialized prior to streaming ingestion."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.silver")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.bronze.bronze_transactions (
            ingest_id STRING,
            order_id STRING,
            customer_id STRING,
            order_status STRING,
            order_purchase_timestamp STRING,
            payment_value STRING,
            ingested_at TIMESTAMP
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.silver.silver_transactions (
            ingest_id STRING,
            order_id STRING,
            customer_id STRING,
            order_status STRING,
            order_purchase_timestamp STRING,
            payment_value STRING,
            parsed_payment_value DECIMAL(18,2),
            created_date DATE,
            validation_status STRING,
            _is_remediated BOOLEAN,
            _remediation_rule STRING
        ) USING iceberg
        PARTITIONED BY (created_date)
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.silver.silver_pending_review (
            ingest_id STRING,
            order_id STRING,
            customer_id STRING,
            order_status STRING,
            order_purchase_timestamp STRING,
            payment_value STRING,
            parsed_payment_value DECIMAL(18,2),
            created_date DATE,
            validation_status STRING,
            _status STRING
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.silver.audit_log (
            order_id STRING,
            old_value STRING,
            new_value STRING,
            field_changed STRING,
            action STRING,
            reviewed_by STRING,
            reviewed_at STRING
        ) USING iceberg
    """)


if __name__ == "__main__":
    spark_session = init_spark_session()
    init_iceberg_tables(spark_session)
    raw_stream = build_kafka_read_stream(spark_session)
    start_streaming_job(spark_session, raw_stream)