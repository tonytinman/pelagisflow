# User Story: NovaFlow Data Operations Dashboard Landing Page

## Overview

**Story ID:** `011NWPwBvVHdsMBxuLFfwCRi`
**Epic:** NovaFlow Observability & Monitoring
**Priority:** High
**Status:** Draft

---

## User Story

**As a** Data Operations Engineer
**I want** a centralized dashboard to monitor NovaFlow pipeline batch executions
**So that** I can quickly identify issues, track performance trends, and ensure data processing SLAs are met

---

## Business Value

### Problem Statement
Data ops teams currently lack a unified view of:
- Which batches are currently running and their progress
- Historical batch execution success/failure rates
- Performance bottlenecks across pipeline stages
- Data quality issue trends over time

### Benefits
- **Reduce MTTR (Mean Time to Resolution):** Identify failed pipelines within minutes instead of hours
- **Proactive Monitoring:** Spot performance degradation before SLAs are breached
- **Operational Efficiency:** Single pane of glass for all NovaFlow executions
- **Data Quality Visibility:** Track DQ error rates and identify problematic data sources

---

## Personas

### Primary: Data Operations Engineer
- Monitors daily batch jobs across dev/test/prod environments
- Responds to pipeline failures and performance issues
- Tracks SLA compliance and escalates blockers
- Performs root cause analysis on data quality failures

### Secondary: Data Platform Lead
- Reviews operational metrics for capacity planning
- Tracks platform reliability and uptime
- Identifies optimization opportunities across pipelines

---

## Acceptance Criteria

### 1. Batch Selection Component
- [ ] **AC1.1:** User can select a batch by `process_queue_id` from a dropdown
- [ ] **AC1.2:** Dropdown shows batch IDs sorted by execution start time (most recent first)
- [ ] **AC1.3:** Dropdown displays batch metadata: ID, start time, environment (dev/test/prod)
- [ ] **AC1.4:** Default selection is the most recent batch
- [ ] **AC1.5:** User can filter batches by date range (last 24h, 7d, 30d, custom)
- [ ] **AC1.6:** User can filter batches by environment

### 2. High-Level Batch Metrics
Display the following metrics for the selected batch:

- [ ] **AC2.1:** Total number of processes (pipelines) in batch
- [ ] **AC2.2:** Number of processes succeeded (status = SUCCESS)
- [ ] **AC2.3:** Number of processes failed (status = FAILED)
- [ ] **AC2.4:** Number of processes in progress (status = RUNNING)
- [ ] **AC2.5:** Number of processes pending (status = PENDING)
- [ ] **AC2.6:** Batch-level success rate percentage
- [ ] **AC2.7:** Visual indicators (color-coded) for health status:
  - Green: 100% success
  - Yellow: 90-99% success
  - Red: < 90% success or any failures
- [ ] **AC2.8:** Metrics refresh automatically every 30 seconds when batch has running processes

### 3. Process List Table
Display a table of all processes in the selected batch with the following columns:

- [ ] **AC3.1:** Process/Contract Name (`data_contract_name`)
- [ ] **AC3.2:** Current Status (PENDING, RUNNING, SUCCESS, FAILED) with color coding
- [ ] **AC3.3:** Start Time (`execution_start`)
- [ ] **AC3.4:** End Time (`execution_end`) - empty if still running
- [ ] **AC3.5:** Duration (calculated: `execution_time_seconds` formatted as HH:MM:SS)
- [ ] **AC3.6:** Rows Read (`rows_read`)
- [ ] **AC3.7:** Rows Written (`rows_written`)
- [ ] **AC3.8:** Rows Updated (from `custom_stats.scd2_changed` if available)
- [ ] **AC3.9:** Rows Inserted (from `custom_stats.scd2_new` if available)
- [ ] **AC3.10:** DQ Errors (`rows_invalid` or `custom_stats.dq_failed_rows`)
- [ ] **AC3.11:** Read Throughput (`read_throughput_rows_per_sec` formatted as rows/sec)
- [ ] **AC3.12:** Write Throughput (`write_throughput_rows_per_sec` formatted as rows/sec)
- [ ] **AC3.13:** Table is sortable by any column
- [ ] **AC3.14:** Table supports pagination (25/50/100 rows per page)
- [ ] **AC3.15:** User can click on a process row to drill down to detailed execution stats

### 4. Data Quality Summary
- [ ] **AC4.1:** Display total DQ errors across all processes in batch
- [ ] **AC4.2:** Show DQ error rate percentage (failed rows / total rows read)
- [ ] **AC4.3:** Highlight processes with DQ errors > 1% as warnings
- [ ] **AC4.4:** Provide link to detailed DQ report (if available)

### 5. Performance & Usability
- [ ] **AC5.1:** Dashboard loads in < 3 seconds for batches with up to 100 processes
- [ ] **AC5.2:** Dashboard is responsive and works on desktop browsers (Chrome, Firefox, Safari)
- [ ] **AC5.3:** Data refreshes automatically without requiring full page reload
- [ ] **AC5.4:** User can export process list to CSV

---

## Technical Approach

### Data Source
**Existing Delta Table:** `{catalog}.nova_framework.pipeline_stats`

**Schema (subset relevant to dashboard):**
```sql
CREATE TABLE pipeline_stats (
  process_queue_id INT,                 -- Batch identifier
  data_contract_name STRING,            -- Process/pipeline name
  execution_start TIMESTAMP,
  execution_end TIMESTAMP,
  execution_time_seconds FLOAT,
  rows_read LONG,
  rows_written LONG,
  rows_invalid LONG,
  read_throughput_rows_per_sec FLOAT,
  write_throughput_rows_per_sec FLOAT,
  custom_stats STRING,                  -- JSON with SCD2, DQ, stage metrics
  timestamp TIMESTAMP
)
```

### Dashboard Queries

#### Query 1: Batch List (for dropdown)
```sql
SELECT DISTINCT
  process_queue_id,
  MIN(execution_start) as batch_start_time,
  MAX(execution_end) as batch_end_time,
  env  -- Assuming env is added to pipeline_stats
FROM {catalog}.nova_framework.pipeline_stats
WHERE execution_start >= CURRENT_DATE() - INTERVAL {days} DAY
GROUP BY process_queue_id, env
ORDER BY batch_start_time DESC
```

#### Query 2: High-Level Batch Metrics
```sql
WITH batch_status AS (
  SELECT
    process_queue_id,
    data_contract_name,
    CASE
      WHEN execution_end IS NULL THEN 'RUNNING'
      WHEN rows_invalid > 0 OR execution_time_seconds IS NULL THEN 'FAILED'
      ELSE 'SUCCESS'
    END as status
  FROM {catalog}.nova_framework.pipeline_stats
  WHERE process_queue_id = :selected_batch_id
)
SELECT
  COUNT(*) as total_processes,
  SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as succeeded,
  SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
  SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END) as in_progress,
  ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate
FROM batch_status
```

#### Query 3: Process List
```sql
SELECT
  data_contract_name as process_name,
  CASE
    WHEN execution_end IS NULL THEN 'RUNNING'
    WHEN rows_invalid > 0 THEN 'FAILED'
    ELSE 'SUCCESS'
  END as status,
  execution_start,
  execution_end,
  execution_time_seconds,
  rows_read,
  rows_written,
  COALESCE(
    CAST(get_json_object(custom_stats, '$.scd2_new') AS LONG),
    0
  ) as rows_inserted,
  COALESCE(
    CAST(get_json_object(custom_stats, '$.scd2_changed') AS LONG),
    0
  ) as rows_updated,
  COALESCE(
    rows_invalid,
    CAST(get_json_object(custom_stats, '$.dq_failed_rows') AS LONG),
    0
  ) as dq_errors,
  read_throughput_rows_per_sec,
  write_throughput_rows_per_sec
FROM {catalog}.nova_framework.pipeline_stats
WHERE process_queue_id = :selected_batch_id
ORDER BY execution_start DESC
```

### Technology Stack Options

#### Option A: Databricks SQL Dashboard (Recommended for MVP)
- **Pros:**
  - Leverages existing `/home/user/pelagisflow/dashboards/pipeline_stats_dashboard.sql`
  - No additional infrastructure required
  - Built-in authentication and access control
  - SQL-based, familiar to data teams
- **Cons:**
  - Limited customization
  - Manual refresh required (no real-time updates)

#### Option B: Streamlit App (Python)
- **Pros:**
  - Full control over UI/UX
  - Real-time updates with auto-refresh
  - Easy to deploy on Databricks
  - Python ecosystem for extensions
- **Cons:**
  - Requires hosting (Databricks Apps or external)
  - Additional maintenance overhead

#### Option C: Web App (React + REST API)
- **Pros:**
  - Modern, responsive UI
  - Full feature set (drill-downs, exports, alerts)
  - Scalable for production use
- **Cons:**
  - Significant development effort
  - Requires separate backend and hosting

**Recommendation:** Start with **Option A** (Databricks SQL Dashboard) for MVP, then migrate to **Option B** (Streamlit) based on feedback.

---

## UI/UX Mockup (Text-Based)

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  NovaFlow Data Operations Dashboard                        [User: jdoe] [🔔] ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  Batch Selection:                                                            ║
║  ┌────────────────────────────────────────────────┐  Filters:               ║
║  │ 🔽 Batch ID: 20231209-001 (Started: 2025-12...) │  [Last 24h ▾] [All Env ▾]║
║  └────────────────────────────────────────────────┘  [🔄 Refresh]           ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Batch Metrics                                            Last Updated: 14:32 ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             ║
║  │  Total Processes │  │   ✅ Succeeded  │  │   ❌ Failed     │             ║
║  │        45       │  │        42       │  │        2        │             ║
║  └─────────────────┘  └─────────────────┘  └─────────────────┘             ║
║                                                                              ║
║  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             ║
║  │  ⏳ In Progress  │  │   ⏸️ Pending     │  │  Success Rate   │             ║
║  │        1        │  │        0        │  │    93.3% 🟡     │             ║
║  └─────────────────┘  └─────────────────┘  └─────────────────┘             ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Process List                                     [Export to CSV] [⚙️ Cols]  ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  ┌────────────────────────────────────────────────────────────────────────┐ ║
║  │ Process Name      Status  Start Time   End Time    Duration  Rows Read │ ║
║  ├────────────────────────────────────────────────────────────────────────┤ ║
║  │ customer_sales    ✅ SUCCESS  14:15:30   14:18:45   00:03:15   1,245,890 │ ║
║  │ product_inventory ❌ FAILED   14:16:10   14:16:25   00:00:15      12,340 │ ║
║  │ order_fulfillment ⏳ RUNNING  14:17:22      --         --       856,320 │ ║
║  │ vendor_payments   ✅ SUCCESS  14:15:45   14:19:12   00:03:27     432,190 │ ║
║  │ ...                                                                      │ ║
║  └────────────────────────────────────────────────────────────────────────┘ ║
║                                                                              ║
║  ┌────────────────────────────────────────────────────────────────────────┐ ║
║  │ Rows Inserted Rows Updated  DQ Errors  Read (r/s)  Write (r/s)         │ ║
║  ├────────────────────────────────────────────────────────────────────────┤ ║
║  │    23,450        1,230         0        6,400       6,380              │ ║
║  │       120           45       12,340 ⚠️    820          10              │ ║
║  │    12,340          890         0       28,500      28,450              │ ║
║  │     5,670          320         0        2,100       2,095              │ ║
║  │    ...                                                                  │ ║
║  └────────────────────────────────────────────────────────────────────────┘ ║
║                                                                              ║
║  Showing 1-25 of 45 processes   [◀️ Prev]  [1] 2 3  [Next ▶️]              ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Data Quality Summary                                                        ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  Total DQ Errors: 12,340  │  DQ Error Rate: 0.48%  │  ⚠️ 1 process > 1%   ║
║  [View Detailed DQ Report →]                                                ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

Legend:
  ✅ Success  ❌ Failed  ⏳ Running  ⏸️ Pending  ⚠️ Warning  🟢 Healthy  🟡 Degraded  🔴 Critical
```

---

## Implementation Tasks

### Phase 1: Data Model & Backend (Week 1)
- [ ] **Task 1.1:** Add `env` and `pipeline_type` fields to `pipeline_stats` table schema
- [ ] **Task 1.2:** Update `PipelineStats.finalize()` to capture process status (SUCCESS/FAILED/RUNNING)
- [ ] **Task 1.3:** Create materialized view `vw_batch_summary` for batch-level metrics
- [ ] **Task 1.4:** Create stored procedure to extract SCD2 metrics from JSON `custom_stats`
- [ ] **Task 1.5:** Implement data retention policy (archive stats > 90 days)

### Phase 2: MVP Dashboard (Week 2)
- [ ] **Task 2.1:** Extend existing `pipeline_stats_dashboard.sql` with batch selection dropdown
- [ ] **Task 2.2:** Create SQL query for high-level batch metrics (Query 2)
- [ ] **Task 2.3:** Create SQL query for process list table (Query 3)
- [ ] **Task 2.4:** Add batch filter component (date range, environment)
- [ ] **Task 2.5:** Configure auto-refresh (30 second interval)
- [ ] **Task 2.6:** Add color coding for status indicators

### Phase 3: Enhanced Features (Week 3)
- [ ] **Task 3.1:** Implement drill-down to detailed process view
- [ ] **Task 3.2:** Add CSV export functionality
- [ ] **Task 3.3:** Create DQ error detail view
- [ ] **Task 3.4:** Implement SLA threshold alerting (email/Slack)
- [ ] **Task 3.5:** Add historical trend charts (7-day, 30-day batch performance)

### Phase 4: Testing & Documentation (Week 4)
- [ ] **Task 4.1:** Write unit tests for SQL queries
- [ ] **Task 4.2:** Perform load testing with 1000+ process batches
- [ ] **Task 4.3:** Create user guide documentation
- [ ] **Task 4.4:** Record demo video for data ops team
- [ ] **Task 4.5:** Conduct UAT with 3 data ops engineers

---

## Dependencies

- **Data Availability:** `pipeline_stats` table must be populated with historical data
- **Access Control:** Users must have READ access to `{catalog}.nova_framework.pipeline_stats`
- **Databricks Workspace:** SQL Warehouse must be running for query execution
- **Schema Changes:** Requires pipeline to `nova_framework` for schema updates

---

## Risks & Mitigations

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Pipeline stats table missing key fields (env, status) | High | Medium | Add fields via schema evolution; backfill historical data |
| Dashboard performance degrades with large batches (1000+ processes) | Medium | High | Implement pagination, indexing on `process_queue_id`, pre-aggregated views |
| Users expect real-time updates (< 5 sec latency) | Medium | Medium | Set expectation for 30-second refresh; future: migrate to Streamlit for real-time |
| DQ metrics inconsistent across pipeline types | Low | Medium | Standardize DQ metric naming in `custom_stats` JSON schema |

---

## Success Metrics

### Usage Metrics
- **Adoption Rate:** 80% of data ops engineers use dashboard daily within 1 month
- **Session Duration:** Average session > 5 minutes (indicates deep engagement)
- **Most Viewed Batches:** Identify top 10 batches by view count

### Operational Metrics
- **MTTR Reduction:** Decrease time to detect failures from 30 min to < 5 min
- **False Positive Alerts:** < 10% of alerts are false positives
- **User Satisfaction:** NPS score > 8 from data ops team

### Performance Metrics
- **Dashboard Load Time:** < 3 seconds for 95th percentile
- **Query Performance:** All queries execute in < 5 seconds

---

## Open Questions

1. **Q:** Should the dashboard support multiple batch selection for comparison?
   **A:** _TBD - Gather feedback from data ops team_

2. **Q:** What SLA thresholds should trigger alerts (execution time, DQ error rate)?
   **A:** _TBD - Define with data platform lead_

3. **Q:** Should we integrate with existing alerting tools (PagerDuty, Opsgenie)?
   **A:** _TBD - Phase 2 enhancement_

4. **Q:** Do we need role-based access control (dev can only see dev batches)?
   **A:** _TBD - Security review required_

---

## Related Documentation

- [NovaFlow Developer Guide](/home/user/pelagisflow/docs/DEVELOPER_GUIDE.md)
- [Existing Pipeline Stats Dashboard](/home/user/pelagisflow/dashboards/pipeline_stats_dashboard.sql)
- [PipelineStats Class](/home/user/pelagisflow/nova_framework/observability/stats.py)
- [ExecutionContext Model](/home/user/pelagisflow/nova_framework/core/models.py)

---

## Changelog

| Date | Author | Change |
|------|--------|--------|
| 2025-12-09 | Claude | Initial draft - comprehensive user story with technical approach |

---

## Approvals

- [ ] **Product Owner:** _[Name]_ - Date: _[YYYY-MM-DD]_
- [ ] **Tech Lead:** _[Name]_ - Date: _[YYYY-MM-DD]_
- [ ] **Data Ops Lead:** _[Name]_ - Date: _[YYYY-MM-DD]_
