#!/bin/bash
# Load environment variables (GCS_BUCKET_NAME, etc.)
export $(grep -v '^#' ../.env | xargs)

# BƯỚC ĐỘT PHÁ: Nhồi thẳng các thư viện từ thư mục tạm (Ivy) vào lõi của PySpark!
echo "Injecting downloaded Jars directly into PySpark Core to bypass Py4J ClassLoader bug..."
PYSPARK_JARS_DIR=$(python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))")
find ~/.ivy2.5.2/jars/ -name "*.jar" -exec cp {} "$PYSPARK_JARS_DIR" \;
echo "Successfully injected Jars into: $PYSPARK_JARS_DIR"

# Cấu hình OS Boot: (Đã gỡ bỏ --packages vì thư viện đã nằm sẵn trong lõi hệ thống)
export PYSPARK_SUBMIT_ARGS="--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.spark_catalog.type=hadoop \
--conf spark.sql.catalog.spark_catalog.warehouse=gs://${GCS_BUCKET_NAME}/warehouse \
--conf spark.sql.catalog.spark_catalog.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
--conf spark.sql.catalog.spark_catalog.hadoop.google.cloud.auth.service.account.enable=true \
--conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
--conf spark.hadoop.google.cloud.auth.service.account.enable=true \
pyspark-shell"

# Execute dbt
dbt run --profiles-dir .
