-- =============================================================================
-- BIGQUERY EXTERNAL TABLES DDL FOR LOOKER STUDIO BI INTEGRATION
-- Run these SQL statements in Google Cloud BigQuery Console Query Editor
-- =============================================================================

-- 1. Create Datasets if not already created (matching GCP Region us-central1)
CREATE SCHEMA IF NOT EXISTS `olist-lakehouse-v25.silver`
OPTIONS (location = 'us-central1');

CREATE SCHEMA IF NOT EXISTS `olist-lakehouse-v25.gold_finance`
OPTIONS (location = 'us-central1');

CREATE SCHEMA IF NOT EXISTS `olist-lakehouse-v25.gold_marketing`
OPTIONS (location = 'us-central1');

-- 2. Expose Silver Cleansed Table to BigQuery
CREATE OR REPLACE EXTERNAL TABLE `olist-lakehouse-v25.silver.silver_transactions`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://olist-streaming-lakehouse-bucket/warehouse/silver/silver_transactions/data/*']
);

-- 3. Expose Gold Finance Daily Revenue Table to BigQuery (For Looker Revenue Dashboards)
CREATE OR REPLACE EXTERNAL TABLE `olist-lakehouse-v25.gold_finance.daily_revenue`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://olist-streaming-lakehouse-bucket/warehouse/gold_finance/daily_revenue/data/*']
);

-- 4. Expose Gold Marketing Customer Activity Table to BigQuery
CREATE OR REPLACE EXTERNAL TABLE `olist-lakehouse-v25.gold_marketing.customer_activity`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://olist-streaming-lakehouse-bucket/warehouse/gold_marketing/customer_activity/data/*']
);

-- =============================================================================
-- 5. SEMANTIC REPORTING VIEWS (For Looker Studio BI Dashboards)
-- These views eliminate historical snapshot file duplicates from external table scans
-- =============================================================================

-- Finance Semantic View (Deduplicated by revenue_date)
CREATE OR REPLACE VIEW `olist-lakehouse-v25.gold_finance.v_daily_revenue` AS
SELECT 
    revenue_date,
    MAX(total_orders) AS total_orders,
    MAX(gross_revenue) AS gross_revenue,
    MAX(average_order_value) AS average_order_value
FROM `olist-lakehouse-v25.gold_finance.daily_revenue`
WHERE revenue_date IS NOT NULL
GROUP BY revenue_date;

-- Marketing Semantic View (Deduplicated by customer_id)
CREATE OR REPLACE VIEW `olist-lakehouse-v25.gold_marketing.v_customer_activity` AS
SELECT 
    customer_id,
    MAX(first_purchase_date) AS first_purchase_date,
    MAX(last_purchase_date) AS last_purchase_date,
    MAX(total_orders) AS total_orders,
    MAX(total_spent) AS total_spent,
    MAX(order_statuses) AS order_statuses
FROM `olist-lakehouse-v25.gold_marketing.customer_activity`
WHERE customer_id IS NOT NULL
GROUP BY customer_id;
