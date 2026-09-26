import argparse

from pyspark.sql import functions as F


parser = argparse.ArgumentParser()

parser.add_argument("--catalog", default="workspace")
parser.add_argument("--schema", default="sales_lab")
parser.add_argument(
    "--source-path",
    default="/Volumes/workspace/sales_lab/landing_sales/orders",
)

args, _ = parser.parse_known_args()

catalog = args.catalog
schema = args.schema
source_path = args.source_path

bronze_table = f"{catalog}.{schema}.bronze_orders_raw"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

raw_orders = (
    spark.read
    .option("header", True)
    .option("recursiveFileLookup", "true")
    .csv(source_path)
)

required_columns = {
    "order_id",
    "customer_id",
    "order_timestamp",
    "product_category",
    "quantity",
    "unit_price",
    "order_status",
}

missing_columns = required_columns - set(raw_orders.columns)

if missing_columns:
    raise ValueError(
        f"Order source file is missing columns: {sorted(missing_columns)}"
    )

bronze_with_metadata = (
    raw_orders
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

if spark.catalog.tableExists(bronze_table):
    already_processed = (
        spark.table(bronze_table)
        .select("_source_file")
        .distinct()
    )

    new_bronze = bronze_with_metadata.join(
        already_processed,
        on="_source_file",
        how="left_anti",
    )
else:
    new_bronze = bronze_with_metadata

new_count = new_bronze.count()

if new_count == 0:
    print("No new order files to process — skipping Bronze append.")
else:
    (
        new_bronze.write
        .format("delta")
        .mode("append")
        .saveAsTable(bronze_table)
    )

    print(f"Order Bronze rows appended: {new_count:,}")