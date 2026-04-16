# Runbook: customer_transactions_daily

**Job:** `customer_transactions_daily.py`
**Owner:** Data Platform Team
**SLA:** Complete within 30 minutes of 02:00 UTC trigger
**Escalation:** #data-platform-oncall Slack channel

---

## 1. Job Overview

This nightly ETL joins `omnitrace.demo.customers` (~50MB, ~10K rows) with `omnitrace.demo.ice_transactions` (~500MB, ~1M rows) and writes the result to `omnitrace.analytics.customer_transactions`.

The join key is `customers.customer_id = transactions.account_id`.

## 2. Common Failure Modes

### 2.1 Cluster Not Starting
**Symptoms:** Job stays in PENDING for >5 minutes
**Cause:** Usually spot instance unavailability or VPC subnet exhaustion
**Fix:** Switch to on-demand instances or retry with a different availability zone

### 2.2 Schema Mismatch
**Symptoms:** `AnalysisException: cannot resolve column`
**Cause:** Upstream table schema changed without updating this job
**Fix:** Check `DESCRIBE TABLE` on both source tables and update the SELECT list

### 2.3 Source Table Empty
**Symptoms:** Job succeeds but writes 0 rows
**Cause:** Upstream job hasn't run yet
**Fix:** Check the `ice_transactions` table freshness — it should have data from yesterday

## 3. Performance Issues

### 3.1 Slow Shuffle (>10 minutes)
**Symptoms:** Stage 7 (SortMergeJoin) takes >10 minutes
**Cause:** Data skew on `customer_id` — a few customers have 100x more transactions
**Fix:** Add `spark.sql.adaptive.skewJoin.enabled=true` to cluster Spark config

### 3.2 Executor OOM
**Symptoms:** `java.lang.OutOfMemoryError: Java heap space` in executor logs
**Cause:** The join between customers and transactions spills to disk when the smaller table isn't broadcast
**Root cause:** Line 47 of `customer_transactions_daily.py` does a regular join instead of a broadcast join

**Fix (immediate):**
```python
# Change this (line 47):
joined = transactions.join(customers, ...)

# To this:
from pyspark.sql.functions import broadcast
joined = transactions.join(broadcast(customers), ...)
```

The `customers` table is only ~50MB — well under the default broadcast threshold of 10MB (which should be raised to 100MB for this cluster). Broadcasting eliminates the shuffle entirely.

**Fix (permanent):**
Set `spark.sql.autoBroadcastJoinThreshold` to `104857600` (100MB) in the cluster's Spark config. This auto-broadcasts any table under 100MB without code changes.

## 4. Table Maintenance

### 4.1 Target Table Compaction
After overwrite, `analytics.customer_transactions` may have many small files if the cluster parallelism is high. Run:
```sql
OPTIMIZE omnitrace.analytics.customer_transactions;
```

### 4.2 Broadcast Hint (Skewed Join Fix)
The customers table is small enough to broadcast. Adding the broadcast hint avoids the shuffle entirely and prevents OOM:
```python
joined = transactions.join(
    F.broadcast(customers),
    transactions["account_id"] == customers["customer_id"],
    "inner"
)
```
This is the single most impactful fix for this job's reliability.

### 4.3 Vacuum Schedule
Run weekly to clean up old overwrite snapshots:
```sql
VACUUM omnitrace.analytics.customer_transactions RETAIN 168 HOURS;
```

## 5. Monitoring

| Metric | Healthy Range | Alert Threshold |
|--------|---------------|-----------------|
| Duration | 5-15 min | >25 min (warn), >30 min (page) |
| Output rows | 8K-12K | <1K (fail), >50K (investigate) |
| Shuffle spill | <100MB | >1GB (tune join) |
| Executor OOM count | 0 | >0 (broadcast fix needed) |

## 6. Rollback

If the job writes bad data:
```sql
-- Restore from Delta time travel (last known good version)
RESTORE TABLE omnitrace.analytics.customer_transactions TO VERSION AS OF <version>;
```
To find the last good version:
```sql
DESCRIBE HISTORY omnitrace.analytics.customer_transactions;
```

## 7. Change Log

| Date | Change | Author |
|------|--------|--------|
| 2026-03-15 | Initial job creation | data-platform-team |
| 2026-04-01 | Added error handling + retry logic | data-platform-team |
| 2026-04-10 | Documented broadcast hint fix (section 4.2) | oncall |
