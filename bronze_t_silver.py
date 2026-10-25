
import csv
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE_INPUT_PATH = "/opt/airflow/data/bronze/trips.csv"
SILVER_OUTPUT_PATH = "/opt/airflow/data/silver/trips.csv"
REJECTED_OUTPUT_PATH = "/opt/airflow/data/silver/rejected.csv"


def write_single_csv(df, output_path):
    """Write a small DataFrame to one CSV file using collect()."""
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


def bronze_to_silver():
    spark = (
        SparkSession.builder
        .appName("ChicagoTaxi-BronzeToSilver")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        raw = spark.read \
            .option("header", True) \
            .option("inferSchema", False) \
            .csv(BRONZE_INPUT_PATH) 
        
        

        # Parse timestamps and numeric measures.
        df = raw
        for col_name in [
            "trip_start_timestamp",
            "trip_end_timestamp",
        ]:
            df = df.withColumn(
                col_name,
                F.to_timestamp(F.col(col_name)),
            )

        numeric_columns = [
            "trip_seconds",
            "trip_miles",
            "fare",
            "tips",
            "tolls",
            "extras",
            "trip_total",
            "pickup_community_area",
            "dropoff_community_area",
            "pickup_centroid_latitude",
            "pickup_centroid_longitude",
            "dropoff_centroid_latitude",
            "dropoff_centroid_longitude",
        ]

        for col_name in numeric_columns:
            df = df.withColumn(
                col_name,
                F.col(col_name).cast("double"),
            )

        # Hard-invalid records: quarantine rather than silently drop.
        invalid = (
            F.col("trip_id").isNull()
            | (F.trim(F.col("trip_id")) == "")
            | F.col("trip_start_timestamp").isNull()
        )

        for col_name in [
            "trip_seconds", "trip_miles",
            "fare", "tips", "tolls", "extras", "trip_total",
        ]:
            invalid = invalid | (
                F.col(col_name).isNotNull()
                & (F.col(col_name) < 0)
            )

        invalid = invalid | (
            F.col("trip_end_timestamp").isNotNull()
            & F.col("trip_start_timestamp").isNotNull()
            & (
                F.col("trip_end_timestamp")
                < F.col("trip_start_timestamp")
            )
        )

        rejected = (
            df.filter(invalid)
            .withColumn(
                "rejection_reason",
                F.concat_ws(
                    "; ",
                    F.when(
                        F.col("trip_id").isNull()
                        | (F.trim(F.col("trip_id")) == ""),
                        F.lit("Missing trip_id"),
                    ),
                    F.when(
                        F.col("trip_start_timestamp").isNull(),
                        F.lit("Invalid start timestamp"),
                    ),
                    *[
                        F.when(
                            F.col(c) < 0,
                            F.lit(f"Negative {c}"),
                        )
                        for c in [
                            "trip_seconds", "trip_miles",
                            "fare", "tips", "tolls", "extras",
                            "trip_total",
                        ]
                    ],
                    F.when(
                        F.col("trip_end_timestamp")
                        < F.col("trip_start_timestamp"),
                        F.lit("End before start"),
                    ),
                ),
            )
        )

        clean = df.filter(~invalid)

        # Deterministic deduplication: keep one row per trip_id.
        # If conflicting duplicates exist, a source update timestamp
        # would be preferable for deciding which record to keep.
        clean = clean.dropDuplicates(["trip_id"])

        # Business-friendly derived fields.
        clean = (
            clean
            .withColumn(
                "trip_date",
                F.to_date("trip_start_timestamp"),
            )
            .withColumn(
                "pickup_hour",
                F.hour("trip_start_timestamp"),
            )
            .withColumn(
                "day_of_week",
                F.date_format("trip_start_timestamp", "EEEE"),
            )
            .withColumn(
                "duration_minutes",
                F.col("trip_seconds") / 60.0,
            )
            .withColumn(
                "speed_mph",
                F.when(
                    F.col("trip_seconds") > 0,
                    F.col("trip_miles") * 3600.0
                    / F.col("trip_seconds"),
                ),
            )
            .withColumn(
                "tip_percentage",
                F.when(
                    F.col("fare") > 0,
                    F.col("tips") * 100.0 / F.col("fare"),
                ),
            )
            .withColumn(
                "revenue_per_mile",
                F.when(
                    F.col("trip_miles") > 0,
                    F.col("trip_total") / F.col("trip_miles"),
                ),
            )
            .withColumn(
                "data_quality_flag",
                F.concat_ws(
                    "; ",
                    F.when(
                        F.col("trip_miles") == 0,
                        F.lit("Zero distance"),
                    ),
                    F.when(
                        F.col("trip_seconds") == 0,
                        F.lit("Zero duration"),
                    ),
                    F.when(
                        (F.col("trip_seconds") > 0)
                        & (F.col("trip_end_timestamp").isNotNull())
                        & (
                            F.col("trip_end_timestamp")
                            == F.col("trip_start_timestamp")
                        ),
                        F.lit("Timestamp-duration mismatch"),
                    ),
                    F.when(
                        F.col("trip_total").isNull(),
                        F.lit("Missing trip total"),
                    ),
                ),
            )
        )

        write_single_csv(clean, SILVER_OUTPUT_PATH)
        write_single_csv(rejected, REJECTED_OUTPUT_PATH)

        print(f"Clean records: {clean.count()}")
        print(f"Rejected records: {rejected.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    bronze_to_silver()
