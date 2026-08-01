-- =============================================================================
-- BIGQUERY EXTERNAL TABLES DDL FOR LOOKER STUDIO BI INTEGRATION
-- Run these SQL statements in Google Cloud BigQuery Console Query Editor
-- =============================================================================

-- 1. Create Datasets if not already created
CREATE SCHEMA IF NOT EXISTS `olist-lakehouse-v25.silver`
OPTIONS (location = 'US');

CREATE SCHEMA IF NOT EXISTS `olist-lakehouse-v25.gold_finance`
OPTIONS (location = 'US');

CREATE SCHEMA IF NOT EXISTS `olist-lakehouse-v25.gold_marketing`
OPTIONS (location = 'US');

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

-- 4. Expose Gold Marketing Customer Activity Table to BigQuery (For Looker Customer Dashboards)
CREATE OR REPLACE EXTERNAL TABLE `olist-lakehouse-v25.gold_marketing.customer_activity`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://olist-streaming-lakehouse-bucket/warehouse/gold_marketing/customer_activity/data/*']
);
