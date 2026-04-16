# Omnitrace Demo Jobs

Sample Databricks jobs for the Omnitrace agentic observability demo.

## Jobs

| Job | Schedule | Purpose |
|-----|----------|---------|
| `customer_transactions_daily.py` | Nightly 2 AM UTC | Joins customers + transactions, writes to `analytics.customer_transactions` |
| `clickstream_aggregator.py` | Hourly | Aggregates raw clickstream events into session-level metrics |
| `data_quality_monitor.py` | Daily 6 AM UTC | Validates row counts, null rates, freshness across key tables |

## Runbooks

| Runbook | Covers |
|---------|--------|
| `customer_transactions_daily.md` | OOM troubleshooting, skewed join fixes, broadcast hints |
| `general_maintenance.md` | OPTIMIZE, VACUUM, small-files remediation |

## Cluster Configuration

See `config/cluster_policies.json` for recommended cluster policy templates.
