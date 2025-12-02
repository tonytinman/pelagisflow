-- ============================================================================
-- Nova Framework Pipeline Statistics Dashboard
-- ============================================================================
--
-- Databricks SQL Dashboard queries for monitoring pipeline performance
-- Table: {catalog}.nova_framework.pipeline_stats
--
-- Setup Instructions:
-- 1. Create a new SQL Dashboard in Databricks
-- 2. Add these queries as visualizations
-- 3. Replace {catalog} with your catalog name (e.g., cluk_dev_nova)
-- ============================================================================

-- ============================================================================
-- QUERY 1: Pipeline Execution Overview (Counter Visualization)
-- ============================================================================
-- Shows total pipelines run, total rows processed, and average execution time
-- Visualization: Counter (3 separate counters)

SELECT
  COUNT(DISTINCT process_queue_id) as total_pipeline_runs,
  SUM(rows_read) as total_rows_read,
  SUM(rows_written) as total_rows_written,
  ROUND(AVG(execution_time_seconds), 2) as avg_execution_time_sec
FROM cluk_dev_nova.nova_framework.pipeline_stats
WHERE timestamp >= CURRENT_DATE - INTERVAL 7 DAYS;


-- ============================================================================
-- QUERY 2: Pipeline Executions Over Time (Line Chart)
-- ============================================================================
-- Tracks pipeline runs, rows processed, and execution time by day
-- Visualization: Line chart with multiple Y-axes

SELECT
  DATE(timestamp) as date,
  COUNT(DISTINCT process_queue_id) as pipeline_runs,
  SUM(rows_read) as total_rows_read,
  SUM(rows_written) as total_rows_written,
  ROUND(AVG(execution_time_seconds), 2) as avg_execution_time_sec
FROM cluk_dev_nova.nova_framework.pipeline_stats
WHERE timestamp >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY DATE(timestamp)
ORDER BY date DESC;


-- ============================================================================
-- QUERY 3: Throughput Metrics (Area Chart)
-- ============================================================================
-- Shows read and write throughput over time
-- Visualization: Area chart

SELECT
  DATE(timestamp) as date,
  HOUR(timestamp) as hour,
  ROUND(AVG(get_json_object(custom_stats, '$.read_throughput_rows_per_sec')), 2) as avg_read_throughput,
  ROUND(AVG(get_json_object(custom_stats, '$.write_throughput_rows_per_sec')), 2) as avg_write_throughput
FROM cluk_dev_nova.nova_framework.pipeline_stats
WHERE timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
  AND custom_stats IS NOT NULL
GROUP BY DATE(timestamp), HOUR(timestamp)
ORDER BY date DESC, hour DESC;


-- ============================================================================
-- QUERY 4: Read vs Write Efficiency (Bar Chart)
-- ============================================================================
-- Compares rows read vs rows written by day
-- Visualization: Grouped bar chart

SELECT
  DATE(timestamp) as date,
  SUM(rows_read) as rows_read,
  SUM(rows_written) as rows_written,
  SUM(rows_invalid) as rows_invalid,
  ROUND(100.0 * SUM(rows_written) / NULLIF(SUM(rows_read), 0), 2) as write_efficiency_pct
FROM cluk_dev_nova.nova_framework.pipeline_stats
WHERE timestamp >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY DATE(timestamp)
ORDER BY date DESC;


-- ============================================================================
-- QUERY 5: SCD2 Operations Breakdown (Stacked Bar Chart)
-- ============================================================================
-- Shows SCD2 operation distribution (new, changed, deleted)
-- Visualization: Stacked bar chart

SELECT
  DATE(timestamp) as date,
  SUM(CAST(get_json_object(custom_stats, '$.scd2_new') AS INT)) as new_records,
  SUM(CAST(get_json_object(custom_stats, '$.scd2_changed') AS INT)) as changed_records,
  SUM(CAST(get_json_object(custom_stats, '$.scd2_deleted') AS INT)) as deleted_records
FROM cluk_dev_nova.nova_framework.pipeline_stats
WHERE timestamp >= CURRENT_DATE - INTERVAL 30 DAYS
  AND custom_stats LIKE '%scd2%'
GROUP BY DATE(timestamp)
ORDER BY date DESC;


-- ============================================================================
-- QUERY 6: Data Quality Metrics (Gauge + Table)
-- ============================================================================
-- Shows DQ failure rate and details
-- Visualization: Gauge for percentage, Table for breakdown

SELECT
  SUM(CAST(get_json_object(custom_stats, '$.dq_total_rows') AS INT)) as total_rows_checked,
  SUM(CAST(get_json_object(custom_stats, '$.dq_failed_rows') AS INT)) as failed_rows,
  ROUND(AVG(CAST(get_json_object(custom_stats, '$.dq_failed_pct') AS DOUBLE)), 2) as avg_failure_pct,
  COUNT(DISTINCT process_queue_id) as pipelines_with_dq
FROM cluk_dev_nova.nova_framework.pipeline_stats
WHERE timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
  AND get_json_object(custom_stats, '$.dq_total_rows') IS NOT NULL;


-- ============================================================================
-- QUERY 7: Stage Performance Breakdown (Horizontal Bar Chart)
-- ============================================================================
-- Shows average duration for each pipeline stage
-- Visualization: Horizontal bar chart

SELECT
  'Read' as stage,
  ROUND(AVG(CAST(get_json_object(custom_stats, '$.stage_Read_duration_seconds') AS DOUBLE)), 2) as avg_duration_sec
FROM cluk_dev_nova.nova_framework.pipeline_stats
WHERE timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
  AND get_json_object(custom_stats, '$.stage_Read_duration_seconds') IS NOT NULL

UNION ALL

SELECT
  'Lineage' as stage,
  ROUND(AVG(CAST(get_json_object(custom_stats, '$.stage_Lineage_duration_seconds') AS DOUBLE)), 2) as avg_duration_sec
FROM cluk_dev_nova.nova_framework.pipeline_stats
WHERE timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
  AND get_json_object(custom_stats, '$.stage_Lineage_duration_seconds') IS NOT NULL

UNION ALL

SELECT
  'Hashing' as stage,
  ROUND(AVG(CAST(get_json_object(custom_stats, '$.stage_Hashing_duration_seconds') AS DOUBLE)), 2) as avg_duration_sec
FROM cluk_dev_nova.nova_framework.pipeline_stats
WHERE timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
  AND get_json_object(custom_stats, '$.stage_Hashing_duration_seconds') IS NOT NULL

UNION ALL

SELECT
  'Deduplication' as stage,
  ROUND(AVG(CAST(get_json_object(custom_stats, '$.stage_Deduplication_duration_seconds') AS DOUBLE)), 2) as avg_duration_sec
FROM cluk_dev_nova.nova_framework.pipeline_stats
WHERE timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
  AND get_json_object(custom_stats, '$.stage_Deduplication_duration_seconds') IS NOT NULL

UNION ALL

SELECT
  'DataQuality' as stage,
  ROUND(AVG(CAST(get_json_object(custom_stats, '$.stage_DataQuality_duration_seconds') AS DOUBLE)), 2) as avg_duration_sec
FROM cluk_dev_nova.nova_framework.pipeline_stats
WHERE timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
  AND get_json_object(custom_stats, '$.stage_DataQuality_duration_seconds') IS NOT NULL

UNION ALL

SELECT
  'Write' as stage,
  ROUND(AVG(CAST(get_json_object(custom_stats, '$.stage_Write_duration_seconds') AS DOUBLE)), 2) as avg_duration_sec
FROM cluk_dev_nova.nova_framework.pipeline_stats
WHERE timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
  AND get_json_object(custom_stats, '$.stage_Write_duration_seconds') IS NOT NULL

ORDER BY avg_duration_sec DESC;


-- ============================================================================
-- QUERY 8: Recent Pipeline Executions (Table)
-- ============================================================================
-- Shows detailed view of recent pipeline runs
-- Visualization: Table

SELECT
  process_queue_id,
  timestamp,
  execution_time_seconds,
  rows_read,
  rows_written,
  rows_invalid,
  ROUND(CAST(get_json_object(custom_stats, '$.read_throughput_rows_per_sec') AS DOUBLE), 2) as read_throughput,
  CAST(get_json_object(custom_stats, '$.dq_failed_rows') AS INT) as dq_failures,
  CAST(get_json_object(custom_stats, '$.scd2_new') AS INT) as scd2_new,
  CAST(get_json_object(custom_stats, '$.scd2_changed') AS INT) as scd2_changed
FROM cluk_dev_nova.nova_framework.pipeline_stats
WHERE timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
ORDER BY timestamp DESC
LIMIT 100;


-- ============================================================================
-- QUERY 9: Execution Time Distribution (Histogram)
-- ============================================================================
-- Shows distribution of pipeline execution times
-- Visualization: Histogram

SELECT
  CASE
    WHEN execution_time_seconds < 60 THEN '0-1 min'
    WHEN execution_time_seconds < 300 THEN '1-5 min'
    WHEN execution_time_seconds < 600 THEN '5-10 min'
    WHEN execution_time_seconds < 1800 THEN '10-30 min'
    ELSE '30+ min'
  END as execution_time_bucket,
  COUNT(*) as pipeline_count
FROM cluk_dev_nova.nova_framework.pipeline_stats
WHERE timestamp >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY
  CASE
    WHEN execution_time_seconds < 60 THEN '0-1 min'
    WHEN execution_time_seconds < 300 THEN '1-5 min'
    WHEN execution_time_seconds < 600 THEN '5-10 min'
    WHEN execution_time_seconds < 1800 THEN '10-30 min'
    ELSE '30+ min'
  END
ORDER BY
  CASE execution_time_bucket
    WHEN '0-1 min' THEN 1
    WHEN '1-5 min' THEN 2
    WHEN '5-10 min' THEN 3
    WHEN '10-30 min' THEN 4
    WHEN '30+ min' THEN 5
  END;


-- ============================================================================
-- QUERY 10: Deduplication Impact (Pie Chart)
-- ============================================================================
-- Shows what percentage of rows are duplicates
-- Visualization: Pie chart

SELECT
  'Unique Rows' as category,
  SUM(rows_read) - SUM(CAST(get_json_object(custom_stats, '$.deduped') AS INT)) as row_count
FROM cluk_dev_nova.nova_framework.pipeline_stats
WHERE timestamp >= CURRENT_DATE - INTERVAL 30 DAYS
  AND get_json_object(custom_stats, '$.deduped') IS NOT NULL

UNION ALL

SELECT
  'Duplicate Rows' as category,
  SUM(CAST(get_json_object(custom_stats, '$.deduped') AS INT)) as row_count
FROM cluk_dev_nova.nova_framework.pipeline_stats
WHERE timestamp >= CURRENT_DATE - INTERVAL 30 DAYS
  AND get_json_object(custom_stats, '$.deduped') IS NOT NULL;


-- ============================================================================
-- QUERY 11: Pipeline Performance Trends (Line Chart with Trend Line)
-- ============================================================================
-- Shows if pipeline performance is improving or degrading over time
-- Visualization: Line chart with trend

SELECT
  DATE(timestamp) as date,
  ROUND(AVG(execution_time_seconds), 2) as avg_execution_time,
  ROUND(AVG(execution_time_seconds / NULLIF(rows_read, 0)), 4) as avg_time_per_row,
  COUNT(*) as runs_per_day
FROM cluk_dev_nova.nova_framework.pipeline_stats
WHERE timestamp >= CURRENT_DATE - INTERVAL 90 DAYS
GROUP BY DATE(timestamp)
ORDER BY date;


-- ============================================================================
-- QUERY 12: Top Slowest Pipeline Executions (Table)
-- ============================================================================
-- Identifies pipelines that need optimization
-- Visualization: Table

SELECT
  process_queue_id,
  timestamp,
  execution_time_seconds,
  rows_read,
  rows_written,
  ROUND(execution_time_seconds / NULLIF(rows_read, 0), 4) as seconds_per_row,
  CAST(get_json_object(custom_stats, '$.stage_Write_duration_seconds') AS DOUBLE) as write_duration_sec,
  CAST(get_json_object(custom_stats, '$.stage_DataQuality_duration_seconds') AS DOUBLE) as dq_duration_sec
FROM cluk_dev_nova.nova_framework.pipeline_stats
WHERE timestamp >= CURRENT_DATE - INTERVAL 30 DAYS
ORDER BY execution_time_seconds DESC
LIMIT 20;


-- ============================================================================
-- Dashboard Layout Recommendations
-- ============================================================================
--
-- Row 1: Overview Metrics
--   - Total Pipeline Runs (Counter)
--   - Total Rows Read (Counter)
--   - Total Rows Written (Counter)
--   - Avg Execution Time (Counter)
--
-- Row 2: Trends
--   - Pipeline Executions Over Time (Line Chart) - Full width
--
-- Row 3: Performance
--   - Throughput Metrics (Area Chart) - 50% width
--   - Execution Time Distribution (Histogram) - 50% width
--
-- Row 4: Operations
--   - SCD2 Operations Breakdown (Stacked Bar) - 50% width
--   - Deduplication Impact (Pie Chart) - 50% width
--
-- Row 5: Quality
--   - Data Quality Metrics (Gauge) - 30% width
--   - Stage Performance Breakdown (Horizontal Bar) - 70% width
--
-- Row 6: Details
--   - Recent Pipeline Executions (Table) - 60% width
--   - Top Slowest Executions (Table) - 40% width
--
-- ============================================================================
