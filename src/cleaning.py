from pyspark.sql import functions as F


def clean_orders(df):
    rows_before = df.count()

    # Drop rows where critical columns are null
    df = df.dropna(subset=["order_id", "customer_id", "order_status"])

    # Remove duplicate orders
    df = df.dropDuplicates(["order_id"])

    # Only keep orders that were actually delivered
    # (removes cancelled, unavailable, etc.)
    df = df.filter(F.col("order_status") == "delivered")

    rows_after = df.count()
    print(f"  Orders:   {rows_before} -> {rows_after} rows "
          f"({rows_before - rows_after} removed)")
    return df


def clean_customers(df):
    rows_before = df.count()

    df = df.dropna(subset=["customer_id", "customer_state"])
    df = df.dropDuplicates(["customer_id"])

    rows_after = df.count()
    print(f"  Customers:{rows_before} -> {rows_after} rows "
          f"({rows_before - rows_after} removed)")
    return df


def clean_payments(df):
    rows_before = df.count()

    # Remove nulls and zero/negative payments
    df = df.dropna(subset=["order_id", "payment_value"])
    df = df.filter(F.col("payment_value") > 0)

    rows_after = df.count()
    print(f"  Payments: {rows_before} -> {rows_after} rows "
          f"({rows_before - rows_after} removed)")
    return df


def clean_items(df):
    rows_before = df.count()

    df = df.dropna(subset=["order_id", "product_id", "price"])
    df = df.filter(F.col("price") > 0)

    rows_after = df.count()
    print(f"  Items:    {rows_before} -> {rows_after} rows "
          f"({rows_before - rows_after} removed)")
    return df


def run_cleaning(orders, customers, payments, items):
    print("\n--- Cleaning Summary ---")
    orders_clean    = clean_orders(orders)
    customers_clean = clean_customers(customers)
    payments_clean  = clean_payments(payments)
    items_clean     = clean_items(items)
    print("--- Cleaning Complete ---\n")
    return orders_clean, customers_clean, payments_clean, items_clean


if __name__ == "__main__":
    from ingestion import create_spark_session, load_raw_data

    spark = create_spark_session()
    orders, customers, payments, items = load_raw_data(spark, "data/raw")
    run_cleaning(orders, customers, payments, items)