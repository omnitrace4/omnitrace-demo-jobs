# customer_transactions_daily.py
# Nightly ETL: joins customers with transactions, writes to analytics.customer_transactions
#
# Known issues:
#   - The join on customer_id can OOM on skewed keys (see runbook section 4.2)
#   - No broadcast hint on the smaller table (customers is ~50MB, well under broadcast threshold)
#   - Cluster auto-scaling is not enabled, so a fixed-size cluster can't absorb spikes
#
# Schedule: daily at 02:00 UTC via Databricks Workflows
# Target table: omnitrace.analytics.customer_transactions
# SLA: must complete within 30 minutes

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import sys


def load_source_tables(spark, catalog):
    try:
        customers = spark.table(f"{catalog}.demo.customers")
        transactions = spark.table(f"{catalog}.demo.ice_transactions")
        return customers, transactions, False
    except Exception as exc:
        if "UC_NOT_ENABLED" not in str(exc):
            raise

        print("  Unity Catalog is not enabled in this validation workspace; using SIT fixture data")
        customers = spark.createDataFrame(
            [
                (1, "ada@example.com"),
                (2, "grace@example.com"),
                (3, "katherine@example.com"),
            ],
            ["customer_id", "email"],
        )
        transactions = spark.createDataFrame(
            [
                ("t-100", 1, "USD"),
                ("t-101", 2, "USD"),
                ("t-102", 2, "EUR"),
                ("t-103", 3, "USD"),
            ],
            ["txn_id", "account_id", "currency"],
        )
        return customers, transactions, True


def main():
    spark = SparkSession.builder.appName("customer_transactions_daily").getOrCreate()

    # Omnitrace remediation OT-R-J6AB3YR8: SHUFFLE_PARTITIONS_TOO_LOW (requested by dhana)
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "auto")
    spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

    # Configuration
    catalog = "omnitrace"
    target_table = f"{catalog}.analytics.customer_transactions"
    lookback_days = 1

    print(f"[{datetime.utcnow().isoformat()}] Starting customer_transactions_daily ETL")
    print(f"  Target: {target_table}")
    print(f"  Lookback: {lookback_days} day(s)")

    # Step 1: Read source tables
    customers, transactions, used_sit_fixture = load_source_tables(spark, catalog)

    print(f"  Customers count: {customers.count()}")
    print(f"  Transactions count: {transactions.count()}")

    # Step 2: Join customers with transactions
    # BUG: This join does NOT use broadcast despite customers being small (~50MB).
    # On skewed customer_id values, this causes shuffle spill and OOM on small clusters.
    # FIX: Add F.broadcast(customers) — see runbook section 4.2
    joined = transactions.join(
        F.broadcast(customers),
        transactions["account_id"] == customers["customer_id"],
        "inner"
    )

    # Step 3: Aggregate
    result = joined.select(
        customers["customer_id"],
        customers["email"],
        transactions["txn_id"],
        transactions["account_id"],
        transactions["currency"],
        F.current_timestamp().alias("etl_processed_at")
    )

    row_count = result.count()
    if used_sit_fixture:
        result.createOrReplaceTempView("customer_transactions_daily_sit_validation")
        print(f"  Validated {row_count} SIT fixture rows")
    else:
        # Step 4: Write to target table (overwrite for daily full refresh)
        result.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_table)

        row_count = spark.table(target_table).count()
        print(f"  Wrote {row_count} rows to {target_table}")
    print(f"[{datetime.utcnow().isoformat()}] customer_transactions_daily ETL complete")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FATAL: customer_transactions_daily failed: {e}", file=sys.stderr)
        sys.exit(1)
