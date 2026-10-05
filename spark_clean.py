import glob
import os
import shutil
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, "data", "taxi")
LOOKUP_FILE = os.path.join(INPUT_DIR, "taxi_zone_lookup.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "spark_full")


spark = (
    SparkSession.builder
    .appName("DA3408-A3-Spark")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.files.maxPartitionBytes", "128m")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def calculate_average_speed(distance, duration_hours):
    if distance is None or duration_hours is None:
        return None

    if distance <= 0 or duration_hours <= 0:
        return None

    return float(distance / duration_hours)


average_speed_udf = F.udf(
    calculate_average_speed,
    DoubleType()
)


start_total = time.perf_counter()


# 1. Prepare output

if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)

os.makedirs(OUTPUT_DIR)


# 2. Location lookup

lookup = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(LOOKUP_FILE)
)

pickup_lookup = lookup.select(
    F.col("LocationID").cast("long").alias("PULocationID"),
    F.col("Zone").alias("pickup_zone"),
    F.col("Borough").alias("pickup_borough"),
)

dropoff_lookup = lookup.select(
    F.col("LocationID").cast("long").alias("DOLocationID"),
    F.col("Zone").alias("dropoff_zone"),
    F.col("Borough").alias("dropoff_borough"),
)


# 3. Process monthly files

input_files = sorted(
    glob.glob(os.path.join(INPUT_DIR, "*.parquet"))
)

print("=" * 70)
print("DA3408 A3 - Spark Data Pipeline")
print("=" * 70)
print(f"Input files: {len(input_files)}")


for index, file in enumerate(input_files, start=1):

    month_start = time.perf_counter()

    print(f"\n[{index}/{len(input_files)}] {os.path.basename(file)}")

    df = spark.read.parquet(file)

    df = df.withColumn(
        "passenger_count",
        F.col("passenger_count").cast("double")
    )

    # Cleansing

    df = df.dropna(
        subset=[
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
            "PULocationID",
            "DOLocationID",
        ]
    )

    df = df.dropDuplicates()

    # Transformation
    df = df.withColumn(
        "trip_duration_hours",
        (
            F.unix_timestamp("tpep_dropoff_datetime")
            - F.unix_timestamp("tpep_pickup_datetime")
        ) / 3600.0
    )

    df = df.withColumn(
        "average_speed_mph",
        average_speed_udf(
            F.col("trip_distance"),
            F.col("trip_duration_hours"),
        )
    )

    df = df.filter(
        F.col("average_speed_mph").isNotNull()
    )
    # Location enrichment

    df = df.join(
        pickup_lookup,
        on="PULocationID",
        how="left",
    )

    df = df.join(
        dropoff_lookup,
        on="DOLocationID",
        how="left",
    )

    # Final columns

    df = df.select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "trip_duration_hours",
        "average_speed_mph",
        "PULocationID",
        "pickup_zone",
        "pickup_borough",
        "DOLocationID",
        "dropoff_zone",
        "dropoff_borough",
        "fare_amount",
        "tip_amount",
        "total_amount",
    )

    # Export

    month_output = os.path.join(
        OUTPUT_DIR,
        f"month_{index:02d}"
    )

    df.write.mode("overwrite").parquet(month_output)

    month_time = time.perf_counter() - month_start

    print(f"Completed in {month_time:.2f} seconds")


# 4. Complete

total_time = time.perf_counter() - start_total

print("\n" + "=" * 70)
print("SPARK BENCHMARK COMPLETE")
print("=" * 70)
print(f"Output directory : {OUTPUT_DIR}")
print(f"Total time       : {total_time:.3f} seconds")
print("=" * 70)

spark.stop()
