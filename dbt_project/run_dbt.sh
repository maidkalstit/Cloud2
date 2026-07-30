#!/bin/bash
# Load environment variables (GCS_BUCKET_NAME, etc.)
export $(grep -v '^#' ../.env | xargs)

# Inject Iceberg Packages, Catalog Configs, and GCS FileSystem Plugs into JVM Boot Arguments
export PYSPARK_SUBMIT_ARGS="--packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.19 \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.demo.type=hadoop \
--conf spark.sql.catalog.demo.warehouse=gs://${GCS_BUCKET_NAME}/warehouse \
--conf spark.sql.defaultCatalog=demo \
--conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
--conf spark.hadoop.google.cloud.auth.service.account.enable=true \
pyspark-shell"

# Execute dbt
dbt run --profiles-dir .
