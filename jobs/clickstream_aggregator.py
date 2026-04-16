# clickstream_aggregator.py
# Hourly ETL: aggregates raw clickstream events into session-level metrics
#
# Schedule: hourly via Databricks Workflows
# Source: omnitrace.demo.clickstream_events
# Target: omnitrace.analytics.session_metrics (append mode)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

def main():
    spark = SparkSession.builder.appName("clickstream_aggregator").getOrCreate()

    catalog = "omnitrace"
    source_table = f"{catalog}.demo.clickstream_events"
    target_table = f"{catalog}.analytics.session_metrics"

    print(f"[{datetime.utcnow().isoformat()}] Starting clickstream_aggregator")

    # Read last hour of events
    cutoff = datetime.utcnow() - timedelta(hours=1)
    events = spark.table(source_table) \
        .filter(F.col("event_timestamp") >= F.lit(cutoff.isoformat()))

    if events.count() == 0:
        print("  No new events in the last hour. Skipping.")
        return

    # Sessionize: group by user_id + session window (30 min gap)
    sessions = events.groupBy(
        "user_id",
        F.window("event_timestamp", "30 minutes").alias("session_window")
    ).agg(
        F.count("*").alias("event_count"),
        F.countDistinct("page_url").alias("unique_pages"),
        F.min("event_timestamp").alias("session_start"),
        F.max("event_timestamp").alias("session_end"),
        F.first("device_type").alias("device_type"),
        F.first("country").alias("country")
    ).withColumn(
        "session_duration_sec",
        F.unix_timestamp("session_end") - F.unix_timestamp("session_start")
    )

    sessions.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(target_table)

    print(f"  Appended {sessions.count()} sessions to {target_table}")
    print(f"[{datetime.utcnow().isoformat()}] clickstream_aggregator complete")


if __name__ == "__main__":
    main()
