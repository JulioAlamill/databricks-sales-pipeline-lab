import argparse

from pyspark.sql import functions as F


parser = argparse.ArgumentParser()

parser.add_argument("--catalog", default="workspace")
parser.add_argument("--schema", default="sales_lab")

args, _ = parser.parse_known_args()

catalog = args.catalog
schema = args.schema

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
    orders.where(
        F.col("order_id").isNull()
        | F.col("customer_id").isNull()
        | (F.col("quantity") <= 0)
        | (F.col("unit_price") <= 0)
        | (F.col("order_amount") <= 0)
    )
    .count()
)

orphan_order_count = (
    orders.select("customer_id")
    .join(
        customers.select("customer_id"),
        on="customer_id",
        how="left_anti",
    )
    .count()
)

assert duplicate_order_count == 0, (
    f"Silver orders has {duplicate_order_count} duplicate order IDs"
)

assert invalid_order_count == 0, (
    f"Silver orders has {invalid_order_count} invalid records"
)

assert orphan_order_count == 0, (
    f"Silver orders has {orphan_order_count} orders without a customer"
)

print(
    "Sales quality checks passed. "
    f"Customers: {customers.count():,}; "
    f"Orders: {orders.count():,}"
)