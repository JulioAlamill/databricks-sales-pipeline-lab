# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "sales_lab")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

customers_table = f"{catalog}.{schema}.silver_customers"
orders_table = f"{catalog}.{schema}.silver_orders"

customers = spark.table(customers_table)
orders = spark.table(orders_table)

duplicate_order_count = (
    orders
    .groupBy("order_id")
    .count()
    .where(F.col("count") > 1)
    .count()
)

invalid_order_count = (
    orders
    .where(
        F.col("order_id").isNull()
        | F.col("customer_id").isNull()
        | (F.col("quantity") <= 0)
        | (F.col("unit_price") <= 0)
        | (F.col("order_amount") <= 0)
    )
    .count()
)

orphan_order_count = (
    orders
    .select("customer_id")
    .join(
        customers.select("customer_id"),
        on="customer_id",
        how="left_anti",
    )
    .count()
)

assert duplicate_order_count == 0, (
    f"Found {duplicate_order_count} duplicate order IDs."
)

assert invalid_order_count == 0, (
    f"Found {invalid_order_count} invalid orders."
)

assert orphan_order_count == 0, (
    f"Found {orphan_order_count} orders without a matching customer."
)

print(
    "Sales quality checks passed. "
    f"Customers: {customers.count():,}; "
    f"Orders: {orders.count():,}"
)
