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

bronze_table = f"{catalog}.{schema}.bronze_customers_raw"
silver_table = f"{catalog}.{schema}.silver_customers"


bronze = spark.table(bronze_table)

silver_source = (
    bronze.select(
        F.upper(F.trim("customer_id")).alias("customer_id"),
        F.trim("customer_name").alias("customer_name"),
        F.lower(F.trim("email")).alias("email"),
        F.upper(F.trim("segment")).alias("segment"),
        F.upper(F.trim("country")).alias("country"),
        F.try_to_date("signup_date", "yyyy-MM-dd").alias("signup_date"),
        F.lower(F.trim("status")).alias("status"),
        F.col("_ingested_at"),
    )
    .where(F.col("customer_id").isNotNull() & (F.length("customer_id") > 0))
    .where(F.col("customer_name").isNotNull() & (F.length("customer_name") > 0))
)


# If a customer arrives more than once, retain the latest ingested record.
dedupe_window = (
    Window
    .partitionBy("customer_id")
    .orderBy(F.col("_ingested_at").desc())
)

silver_source = (
    silver_source
    .withColumn("_row_number", F.row_number().over(dedupe_window))
    .where(F.col("_row_number") == 1)
    .drop("_row_number")
)


# First run creates the table. Later runs upsert by customer_id.
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

print(f"Customer Silver rows: {spark.table(silver_table).count():,}")