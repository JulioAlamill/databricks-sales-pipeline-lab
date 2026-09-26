from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "sales_lab")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

bronze_table = f"{catalog}.{schema}.bronze_orders_raw"
silver_table = f"{catalog}.{schema}.silver_orders"

bronze = spark.table(bronze_table)

silver_source = (
    bronze
    .select(
        F.upper(F.trim("order_id")).alias("order_id"),
        F.upper(F.trim("customer_id")).alias("customer_id"),
        F.to_timestamp("order_timestamp").alias("order_timestamp"),
        F.to_date(F.to_timestamp("order_timestamp")).alias("order_date"),
        F.trim("product_category").alias("product_category"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("double").alias("unit_price"),
        F.lower(F.trim("order_status")).alias("order_status"),
        F.col("_ingested_at"),
    )
    .where(F.col("order_id").isNotNull())
    .where(F.length("order_id") > 0)
    .where(F.col("customer_id").isNotNull())
    .where(F.col("order_timestamp").isNotNull())
    .where(F.col("quantity") > 0)
    .where(F.col("unit_price") > 0)
)

# Keep the most recently ingested version of each order.
order_window = (
    Window
    .partitionBy("order_id")
    .orderBy(F.col("_ingested_at").desc())
)

silver_source = (
    silver_source
    .withColumn("_row_number", F.row_number().over(order_window))
    .where(F.col("_row_number") == 1)
    .drop("_row_number")
    .withColumn(
        "order_amount",
        F.col("quantity") * F.col("unit_price"),
    )
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

print(f"Order Silver rows: {silver_source.count():,}")
