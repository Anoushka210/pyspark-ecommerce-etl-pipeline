import os
import time
from ingestion import create_spark_session, load_raw_data
from cleaning import run_cleaning
from transformation import run_transformation
from aggregation import run_aggregations


def save_outputs(master, rev_state, monthly, delivery, buckets, output_path):
    print("\n--- Saving Outputs ---")

    # Save master cleaned dataset as Parquet
    master_path = f"{output_path}/master_cleaned.parquet"
    master.write.mode("overwrite").parquet(master_path)
    print(f"  Master dataset saved -> {master_path}")

    # Save each aggregation as a single CSV file
    rev_state.coalesce(1).write.mode("overwrite").csv(
        f"{output_path}/revenue_by_state", header=True
    )
    print(f"  Revenue by state   -> {output_path}/revenue_by_state/")

    monthly.coalesce(1).write.mode("overwrite").csv(
        f"{output_path}/monthly_trend", header=True
    )
    print(f"  Monthly trend      -> {output_path}/monthly_trend/")

    delivery.coalesce(1).write.mode("overwrite").csv(
        f"{output_path}/delivery_performance", header=True
    )
    print(f"  Delivery perf      -> {output_path}/delivery_performance/")

    buckets.coalesce(1).write.mode("overwrite").csv(
        f"{output_path}/order_value_buckets", header=True
    )
    print(f"  Value buckets      -> {output_path}/order_value_buckets/")

    print("--- Outputs Saved ---\n")


def run_pipeline():
    start_time = time.time()

    print("=" * 50)
    print("   E-COMMERCE ETL PIPELINE")
    print("=" * 50)

    # Step 1 — Ingestion
    spark = create_spark_session()
    orders, customers, payments, items = load_raw_data(spark, "data/raw")

    # Step 2 — Cleaning
    orders, customers, payments, items = run_cleaning(
        orders, customers, payments, items
    )

    # Step 3 — Transformation
    master = run_transformation(orders, customers, payments, items)

    # Step 4 — Aggregation
    rev_state, monthly, delivery, buckets = run_aggregations(master)

    # Step 5 — Save outputs
    os.makedirs("data/output", exist_ok=True)
    save_outputs(master, rev_state, monthly, delivery,
                 buckets, "data/output")

    elapsed = round(time.time() - start_time, 1)
    print("=" * 50)
    print(f"   PIPELINE COMPLETE in {elapsed}s")
    print("=" * 50)


if __name__ == "__main__":
    run_pipeline()