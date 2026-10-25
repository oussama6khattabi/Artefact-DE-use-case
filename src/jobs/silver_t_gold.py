
import csv
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_INPUT_PATH = "/opt/airflow/data/silver/trips.csv"
GOLD_DIR = Path("/opt/airflow/data/gold")


def write_single_csv(df, output_path):
    """Collect an aggregated DataFrame and write a single CSV."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    rows = df.collect()

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(df.columns)

        for row in rows:
            writer.writerow([
                str(value) if value is not None else ""
                for value in row
            ])

    print(f"Wrote {len(rows)} rows to {output_path}")


def silver_to_gold():
    spark = (
        SparkSession.builder
        .appName("ChicagoTaxi-SilverToGold")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        # Read the enriched Silver CSV.
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(SILVER_INPUT_PATH)
        )

        # Restore important data types after reading CSV.
        df = (
            df
            .withColumn(
                "trip_start_timestamp",
                F.to_timestamp("trip_start_timestamp"),
            )
            .withColumn("trip_date", F.to_date("trip_date"))
        )

        for col_name in [
            "trip_seconds", "trip_miles", "fare",
            "tips", "trip_total", "pickup_hour",
            "speed_mph", "tip_percentage",
            "duration_minutes", "revenue_per_mile",
        ]:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    F.col(col_name).cast("double"),
                )

        # 1. Daily business performance
        daily = (
            df.groupBy("trip_date")
            .agg(
                F.count("*").alias("trip_count"),
                F.sum("trip_total").alias("total_revenue"),
                F.avg("trip_total").alias("average_trip_value"),
                F.avg("duration_minutes").alias("avg_duration_minutes"),
                F.avg("trip_miles").alias("avg_trip_miles"),
                F.avg("speed_mph").alias("avg_speed_mph"),
                F.avg("tip_percentage").alias("avg_tip_percentage"),
            )
            .orderBy("trip_date")
        )

        write_single_csv(
            daily,
            GOLD_DIR / "daily_performance.csv",
        )

        # 2. Hourly demand: identify busy periods
        hourly = (
            df.groupBy("pickup_hour")
            .agg(
                F.count("*").alias("trip_count"),
                F.sum("trip_total").alias("total_revenue"),
                F.avg("trip_total").alias("average_trip_value"),
                F.avg("duration_minutes").alias("avg_duration_minutes"),
            )
            .orderBy("pickup_hour")
        )

        write_single_csv(
            hourly,
            GOLD_DIR / "hourly_demand.csv",
        )

        # 3. Pickup zone performance
        zones = (
            df.filter(F.col("pickup_community_area").isNotNull())
            .groupBy("pickup_community_area")
            .agg(
                F.count("*").alias("trip_count"),
                F.sum("trip_total").alias("total_revenue"),
                F.avg("trip_total").alias("average_trip_value"),
                F.avg("trip_miles").alias("avg_trip_miles"),
                F.avg("tip_percentage").alias("avg_tip_percentage"),
            )
            .orderBy(F.desc("trip_count"))
        )

        write_single_csv(
            zones,
            GOLD_DIR / "pickup_zone_performance.csv",
        )

        # 4. Taxi company performance
        companies = (
            df.filter(
                F.col("company").isNotNull()
                & (F.trim(F.col("company")) != "")
            )
            .groupBy("company")
            .agg(
                F.count("*").alias("trip_count"),
                F.sum("trip_total").alias("total_revenue"),
                F.avg("trip_total").alias("average_trip_value"),
                F.avg("duration_minutes").alias("avg_duration_minutes"),
                F.avg("tip_percentage").alias("avg_tip_percentage"),
                F.avg("speed_mph").alias("avg_speed_mph"),
            )
            .orderBy(F.desc("total_revenue"))
        )

        write_single_csv(
            companies,
            GOLD_DIR / "company_performance.csv",
        )

        # 5. Most popular routes
        routes = (
            df.filter(
                F.col("pickup_community_area").isNotNull()
                & F.col("dropoff_community_area").isNotNull()
            )
            .groupBy(
                "pickup_community_area",
                "dropoff_community_area",
            )
            .agg(
                F.count("*").alias("trip_count"),
                F.sum("trip_total").alias("total_revenue"),
                F.avg("trip_miles").alias("avg_trip_miles"),
                F.avg("duration_minutes").alias("avg_duration_minutes"),
            )
            .orderBy(F.desc("trip_count"))
        )

        write_single_csv(
            routes,
            GOLD_DIR / "popular_routes.csv",
        )

        print("All Gold aggregations completed.")

    finally:
        spark.stop()


if __name__ == "__main__":
    silver_to_gold()
