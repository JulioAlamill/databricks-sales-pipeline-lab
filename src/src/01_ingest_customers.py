from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "sales_lab")
dbutils.widgets.text(
    "source_path",
    "/Volumes/workspace/sales_lab/landing_sales/customers"
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_path = dbutils.widgets.get("source_path")

bronze_table = f"{catalog}.{schema}.bronze_customers_raw"

raw_customers = (
    spark.read
    .option("header", True)
    .option("recursiveFileLookup", "true")
    .csv(source_path)
)

required_columns = {
    "customer_id", "customer_name", "email", "segment",
    "country", "signup_date", "status",
}

missing_columns = required_columns - set(raw_customers.columns)

if missing_columns:
    raise ValueError(
        f"Customer source file is missing columns: {sorted(missing_columns)}"
    )

bronze_with_metadata = (
    raw_customers
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
    print("No new customer files to process — skipping Bronze append.")
else:
    (
        new_bronze.write
        .format("delta")
        .mode("append")
        .saveAsTable(bronze_table)
    )

    print(f"Customer Bronze rows appended: {new_count:,}")
