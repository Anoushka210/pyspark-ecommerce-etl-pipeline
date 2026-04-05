from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)

ORDERS_SCHEMA = StructType([
    StructField("order_id",                       StringType(),    True),
    StructField("customer_id",                    StringType(),    True),
    StructField("order_status",                   StringType(),    True),
    StructField("order_purchase_timestamp",       TimestampType(), True),
    StructField("order_approved_at",              TimestampType(), True),
    StructField("order_delivered_carrier_date",   TimestampType(), True),
    StructField("order_delivered_customer_date",  TimestampType(), True),
    StructField("order_estimated_delivery_date",  TimestampType(), True),
])

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id",              StringType(),  True),
    StructField("customer_unique_id",       StringType(),  True),
    StructField("customer_zip_code_prefix", IntegerType(), True),
    StructField("customer_city",            StringType(),  True),
    StructField("customer_state",           StringType(),  True),
])

PAYMENTS_SCHEMA = StructType([
    StructField("order_id",              StringType(),  True),
    StructField("payment_sequential",    IntegerType(), True),
    StructField("payment_type",          StringType(),  True),
    StructField("payment_installments",  IntegerType(), True),
    StructField("payment_value",         DoubleType(),  True),
])

ITEMS_SCHEMA = StructType([
    StructField("order_id",           StringType(),    True),
    StructField("order_item_id",      IntegerType(),   True),
    StructField("product_id",         StringType(),    True),
    StructField("seller_id",          StringType(),    True),
    StructField("shipping_limit_date", TimestampType(), True),
    StructField("price",              DoubleType(),    True),
    StructField("freight_value",      DoubleType(),    True),
])