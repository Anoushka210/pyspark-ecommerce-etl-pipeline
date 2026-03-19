from pyspark.sql import functions as F


def revenue_by_state(master):
    return master.groupBy("customer_state").agg(
        F.round(F.sum("total_payment"), 2).alias("total_revenue"),
        F.count("order_id").alias("order_count"),
        F.round(F.avg("total_payment"), 2).alias("avg_order_value")
    ).orderBy("total_revenue", ascending=False)


def monthly_sales_trend(master):
    return master.groupBy("purchase_year", "purchase_month").agg(
        F.round(F.sum("total_payment"), 2).alias("monthly_revenue"),
        F.count("order_id").alias("order_count")
    ).orderBy("purchase_year", "purchase_month")


def delivery_performance_by_state(master):
    return master.filter(
        F.col("delivery_time_days").isNotNull()
    ).groupBy("customer_state").agg(
        F.round(F.avg("delivery_time_days"), 1).alias("avg_delivery_days"),
        F.round(F.avg("delivery_delay_days"), 1).alias("avg_delay_days"),
        F.round(
            F.sum(F.col("was_late").cast("int")) /
            F.count("order_id") * 100, 1
        ).alias("late_delivery_pct")
    ).orderBy("avg_delivery_days")


def order_value_distribution(master):
    return master.groupBy("order_value_bucket").agg(
        F.count("order_id").alias("order_count"),
        F.round(F.sum("total_payment"), 2).alias("total_revenue"),
        F.round(F.avg("total_payment"), 2).alias("avg_value")
    ).orderBy("avg_value")


def run_aggregations(master):
    print("\n--- Aggregation Summary ---")

    rev_state   = revenue_by_state(master)
    monthly     = monthly_sales_trend(master)
    delivery    = delivery_performance_by_state(master)
    buckets     = order_value_distribution(master)

    print("\n  Top 5 states by revenue:")
    rev_state.show(5)

    print("  Monthly trend (first 5 months):")
    monthly.show(5)

    print("  Delivery performance (fastest 5 states):")
    delivery.show(5)

    print("  Order value distribution:")
    buckets.show()

    print("--- Aggregations Complete ---\n")

    return rev_state, monthly, delivery, buckets


if __name__ == "__main__":
    from ingestion import create_spark_session, load_raw_data
    from cleaning import run_cleaning
    from transformation import run_transformation

    spark = create_spark_session()
    orders, customers, payments, items = load_raw_data(spark, "data/raw")
    orders, customers, payments, items = run_cleaning(orders, customers,
                                                      payments, items)
    master = run_transformation(orders, customers, payments, items)
    run_aggregations(master)