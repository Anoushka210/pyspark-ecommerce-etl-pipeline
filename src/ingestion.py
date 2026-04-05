from pyspark.sql import SparkSession
from schema import (
    ORDERS_SCHEMA, CUSTOMERS_SCHEMA,
    PAYMENTS_SCHEMA, ITEMS_SCHEMA
)


def create_spark_session():
    spark = SparkSession.builder \
        .appName("EcommerceETL") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_raw_data(spark, raw_path):
    orders = spark.read.csv(
        f"{raw_path}/olist_orders_dataset.csv",
        header=True, schema=ORDERS_SCHEMA
    )
    customers = spark.read.csv(
        f"{raw_path}/olist_customers_dataset.csv",
        header=True, schema=CUSTOMERS_SCHEMA
    )
    payments = spark.read.csv(
        f"{raw_path}/olist_order_payments_dataset.csv",
        header=True, schema=PAYMENTS_SCHEMA
    )
    items = spark.read.csv(
        f"{raw_path}/olist_order_items_dataset.csv",
        header=True, schema=ITEMS_SCHEMA
    )
    return orders, customers, payments, items


def log_ingestion_summary(orders, customers, payments, items):
    print("\n--- Ingestion Summary ---")
    print(f"Orders:    {orders.count()} rows")
    print(f"Customers: {customers.count()} rows")
    print(f"Payments:  {payments.count()} rows")
    print(f"Items:     {items.count()} rows")
    print("--- Ingestion Complete ---\n")


if __name__ == "__main__":
    spark = create_spark_session()
    orders, customers, payments, items = load_raw_data(spark, "data/raw")
    log_ingestion_summary(orders, customers, payments, items)