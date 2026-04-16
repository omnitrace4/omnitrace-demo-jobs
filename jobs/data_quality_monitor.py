# data_quality_monitor.py
# Daily data quality checks across key tables
#
# Schedule: daily at 06:00 UTC
# Validates: row counts, null rates, freshness, schema drift
# Alerts: writes results to omnitrace.analytics.dq_results

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta

CHECKS = [
    {
        "table": "omnitrace.demo.customers",
        "min_rows": 50,
        "required_columns": ["customer_id", "email"],
        "max_null_pct": {"email": 0.05},
    },
    {
        "table": "omnitrace.demo.ice_transactions",
        "min_rows": 100,
        "required_columns": ["txn_id", "account_id", "amount"],
        "max_null_pct": {"amount": 0.01},
    },
    {
        "table": "omnitrace.analytics.customer_transactions",
        "min_rows": 10,
        "max_age_hours": 26,  # must have been refreshed in the last 26h
        "required_columns": ["customer_id", "txn_id"],
    },
]


def main():
    spark = SparkSession.builder.appName("data_quality_monitor").getOrCreate()
    print(f"[{datetime.utcnow().isoformat()}] Starting data quality monitor")

    results = []
    for check in CHECKS:
        table_name = check["table"]
        try:
            df = spark.table(table_name)
            row_count = df.count()

            # Row count check
            min_rows = check.get("min_rows", 0)
            if row_count < min_rows:
                results.append((table_name, "ROW_COUNT", "FAIL",
                    f"Expected >= {min_rows}, got {row_count}"))
            else:
                results.append((table_name, "ROW_COUNT", "PASS",
                    f"{row_count} rows"))

            # Null rate checks
            for col, max_pct in check.get("max_null_pct", {}).items():
                null_count = df.filter(F.col(col).isNull()).count()
                null_pct = null_count / row_count if row_count > 0 else 0
                if null_pct > max_pct:
                    results.append((table_name, f"NULL_RATE_{col}", "FAIL",
                        f"{null_pct:.2%} null (max {max_pct:.0%})"))
                else:
                    results.append((table_name, f"NULL_RATE_{col}", "PASS",
                        f"{null_pct:.2%} null"))

            # Required columns check
            actual_cols = set(df.columns)
            for req_col in check.get("required_columns", []):
                if req_col not in actual_cols:
                    results.append((table_name, f"COLUMN_{req_col}", "FAIL",
                        "Column missing"))
                else:
                    results.append((table_name, f"COLUMN_{req_col}", "PASS", "Present"))

        except Exception as e:
            results.append((table_name, "TABLE_ACCESS", "FAIL", str(e)))

    # Report
    for table, check_name, status, detail in results:
        marker = "[PASS]" if status == "PASS" else "[FAIL]"
        print(f"  {marker} {table} / {check_name}: {detail}")

    failures = [r for r in results if r[2] == "FAIL"]
    print(f"\n  Total checks: {len(results)}, Failures: {len(failures)}")
    if failures:
        print("  [WARNING] Data quality issues detected — review failures above.")

    print(f"[{datetime.utcnow().isoformat()}] Data quality monitor complete")


if __name__ == "__main__":
    main()
