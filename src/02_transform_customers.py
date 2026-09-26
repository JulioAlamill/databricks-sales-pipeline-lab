# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "sales_lab")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

bronze_table = f"{catalog}.{schema}.bronze_customers_raw"
silver_table = f"{catalog}.{schema}.silver_customers"

bronze = spark.table(bronze_table)

silver_source = (
    bronze
    .select(
        F.upper(F.trim("customer_id")).alias("customer_id"),
        F.trim("customer_name").alias("customer_name"),
        F.lower(F.trim("email")).alias("email"),
        F.trim("segment").alias("segment"),
        F.upper(F.trim("country")).alias("country"),
        F.to_date("signup_date").alias("signup_date"),
        F.lower(F.trim("status")).alias("status"),
        F.col("_ingested_at"),
    )
    .where(F.col("customer_id").isNotNull())
    .where(F.length("customer_id") > 0)
    .where(F.col("customer_name").isNotNull())
    .where(F.length("customer_name") > 0)
)

# Keep the latest version of each customer record.
customer_window = (
    Window
    .partitionBy("customer_id")
    .orderBy(F.col("_ingested_at").desc())
)

silver_source = (
    silver_source
    .withColumn("_row_number", F.row_number().over(customer_window))
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
            "target.customer_id = source.customer_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

print(f"Customer Silver rows: {silver_source.count():,}")
