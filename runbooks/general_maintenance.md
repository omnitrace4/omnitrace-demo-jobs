# Runbook: General Delta/Iceberg Table Maintenance

**Scope:** All tables in the `omnitrace` Unity Catalog
**Owner:** Data Platform Team
**Schedule:** Weekly review, immediate action for CRITICAL tables

---

## 1. Small Files Remediation

**Symptom:** Table health score drops below 50, avg file size < 32MB
**Impact:** Query slowdowns, excessive S3 LIST API costs, metadata overhead

**Fix:**
```sql
-- For Delta tables:
OPTIMIZE <catalog>.<schema>.<table>;

-- For Iceberg tables:
CALL system.rewrite_data_files('<catalog>.<schema>.<table>');
```

**Prevention:**
```sql
ALTER TABLE <table> SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);
```

## 2. VACUUM Schedule

Run VACUUM weekly to clean up stale snapshots and reduce storage costs:
```sql
-- Delta: default 7-day retention
VACUUM <table> RETAIN 168 HOURS;

-- Iceberg: expire old snapshots
CALL system.expire_snapshots('<table>', TIMESTAMP '...');
```

**Warning:** Never set retention below your longest-running query's duration.

## 3. OPTIMIZE + Z-ORDER Strategy

| Table Pattern | Z-ORDER Columns | Rationale |
|--------------|-----------------|-----------|
| Fact tables (transactions, events) | `event_date, customer_id` | Time-range + entity lookup |
| Dimension tables (customers, products) | `customer_id` or `product_id` | Primary key lookups |
| Aggregation tables (session_metrics) | `session_start, user_id` | Time-series queries |

## 4. Health Score Thresholds

| Score | Status | Action |
|-------|--------|--------|
| 80-100 | HEALTHY | No action needed |
| 50-79 | WARNING | Schedule maintenance within 1 week |
| 20-49 | DEGRADED | Schedule maintenance within 2 days |
| 0-19 | CRITICAL | Fix immediately — active query degradation |

## 5. Cluster Policy Enforcement

All production clusters must have:
- Auto-termination: 15-30 minutes
- Cluster policy attached (prevents oversized instances)
- Tags: `env`, `team`, `project` at minimum
- Spot instances for non-SLA workloads

## 6. Cost Attribution Tags

Required tags for chargeback:
```json
{
  "env": "prod|staging|dev",
  "team": "data-platform|analytics|ml",
  "project": "cost-optimization|etl-reliability|ml-training",
  "cost_center": "CC-1234"
}
```
