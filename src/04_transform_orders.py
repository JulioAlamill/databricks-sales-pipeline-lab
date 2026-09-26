import argparse

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F


parser = argparse.ArgumentParser()

parser.add_argument("--catalog", default="workspace")
parser.add_argument("--schema", default="sales_lab")

args, _ = parser.parse_known_args()

catalog = args.catalog
schema = args.schema

bronze_table = f"{catalog}.{schema}.bronze_orders_raw"
silver_table = f"{catalog}.{schema}.silver_orders"

bronze = spark.table(bronze_table)

silver_source = (
    bronze.select(
        F.upper(F.trim("order_id")).alias("order_id"),
        F.upper(F.trim("customer_id")).alias("customer_id"),
        F.try_to_timestamp(
            F.col("order_timestamp"),
            F.lit("yyyy-MM-dd HH:mm:ss"),
        ).alias("order_timestamp"),
        F.trim("product_category").alias("product_category"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("double").alias("unit_price"),
        F.lower(F.trim("order_status")).alias("order_status"),
        F.col("_ingested_at"),
    )
    .where(F.col("order_id").isNotNull() & (F.length("order_id") > 0))
    .where(F.col("customer_id").isNotNull() & (F.length("customer_id") > 0))
    .where(F.col("order_timestamp").isNotNull())
    .where(F.col("quantity") > 0)
    .where(F.col("unit_price") > 0)
    .withColumn("order_date", F.to_date("order_timestamp"))
    .withColumn("order_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
)

dedupe_window = (
    Window
    .partitionBy("order_id")
    .orderBy(F.col("_ingested_at").desc())
)

silver_source = (
    silver_source
    .withColumn("_row_number", F.row_number().over(dedupe_window))
    .where(F.col("_row_number") == 1)
    .drop("_row_number")
)

if not spark.catalog.tableExists(silver_table):
    (
        silver_source.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
    )
else:
    target = DeltaTable.forName(spark, silver_table)

    (
        target.alias("target")
        .merge(
            silver_source.alias("source"),
            "target.order_id = source.order_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

print(f"Order Silver rows: {spark.table(silver_table).count():,}")