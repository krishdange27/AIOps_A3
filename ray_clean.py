import os
import time
import shutil

import pandas as pd
import ray
from ray import data


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

INPUT_DIR = os.path.join(BASE_DIR, "data", "taxi")
LOOKUP_FILE = os.path.join(INPUT_DIR, "taxi_zone_lookup.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "ray_full")

if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)


ray.init(address="auto")

print("=" * 70)
print("DA3408 A3 - Ray Data Pipeline")
print("=" * 70)

total_start = time.perf_counter()


# 1. Ingestion

required_columns = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "tip_amount",
    "total_amount",
]

ds = data.read_parquet(
    INPUT_DIR,
    columns=required_columns,
    override_num_blocks=16,
)


# 2. Cleansing

def valid_row(row):
    return (
        row["tpep_pickup_datetime"] is not None
        and row["tpep_dropoff_datetime"] is not None
        and row["trip_distance"] is not None
        and row["PULocationID"] is not None
        and row["DOLocationID"] is not None
    )


ds = ds.filter(
    valid_row,
    num_cpus=1,
)


def remove_batch_duplicates(batch):
    return batch.drop_duplicates()


ds = ds.map_batches(
    remove_batch_duplicates,
    batch_format="pandas",
    batch_size=10000,
    num_cpus=1,
)


# 3. Transformation

def calculate_average_speed(distance, duration_hours):
    if distance is None or duration_hours is None:
        return None

    if duration_hours <= 0:
        return None

    return distance / duration_hours


def add_speed(batch):
    pickup = pd.to_datetime(
        batch["tpep_pickup_datetime"],
        errors="coerce",
    )

    dropoff = pd.to_datetime(
        batch["tpep_dropoff_datetime"],
        errors="coerce",
    )

    duration_hours = (
        (dropoff - pickup).dt.total_seconds() / 3600.0
    )

    batch["trip_duration_hours"] = duration_hours

    batch["average_speed_mph"] = [
        calculate_average_speed(distance, duration)
        for distance, duration in zip(
            batch["trip_distance"],
            duration_hours,
        )
    ]

    return batch


ds = ds.map_batches(
    add_speed,
    batch_format="pandas",
    batch_size=10000,
    num_cpus=1,
)

ds = ds.filter(
    lambda row: row["average_speed_mph"] is not None,
    num_cpus=1,
)


# 4. Location enrichment

lookup_df = pd.read_csv(LOOKUP_FILE)

zone_lookup = {}

for _, row in lookup_df.iterrows():
    location_id = int(row["LocationID"])

    zone_lookup[location_id] = {
        "zone": row["Zone"],
        "borough": row["Borough"],
    }


def add_location_information(batch):
    batch["pickup_zone"] = [
        zone_lookup.get(int(x), {}).get("zone")
        for x in batch["PULocationID"]
    ]

    batch["pickup_borough"] = [
        zone_lookup.get(int(x), {}).get("borough")
        for x in batch["PULocationID"]
    ]

    batch["dropoff_zone"] = [
        zone_lookup.get(int(x), {}).get("zone")
        for x in batch["DOLocationID"]
    ]

    batch["dropoff_borough"] = [
        zone_lookup.get(int(x), {}).get("borough")
        for x in batch["DOLocationID"]
    ]

    return batch


ds = ds.map_batches(
    add_location_information,
    batch_format="pandas",
    batch_size=10000,
    num_cpus=1,
)


# 5. Export

final_columns = [
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
]

ds = ds.select_columns(final_columns)

ds.write_parquet(
    OUTPUT_DIR,
    mode="overwrite",
)

total_time = time.perf_counter() - total_start

print("\n" + "=" * 70)
print("RAY BENCHMARK COMPLETE")
print("=" * 70)
print(f"Output directory : {OUTPUT_DIR}")
print(f"Total time       : {total_time:.3f} seconds")
print("=" * 70)
