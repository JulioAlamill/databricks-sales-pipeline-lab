import argparse

from pyspark.sql import functions as F


# Python script task parameters.
# Defaults let you also run the script without supplying job parameters.
parser = argparse.ArgumentParser()

parser.add_argument("--catalog", default="workspace")
parser.add_argument("--schema", default="sales_lab")
parser.add_argument(
    "--source-path",
    default="/Volumes/workspace/sales_lab/landing_sales/customers",
)

args, _ = parser.parse_known_args()

catalog = args.catalog
schema = args.schema
source_path = args.source_path

bronze_table = f"{catalog}.{schema}.bronze_customers_raw"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


# Read every CSV currently present in the customers landing folder.
raw_customers = (
    spark.read
    .option("header", True)
    .option("recursiveFileLookup", "true")
    .csv(source_path)
)

required_columns = {
    "customer_id",
    "customer_name",
    "email",
    "segment",
    "country",
    "signup_date",
    "status",
}

missing_columns = required_columns - set(raw_customers.columns)

if missing_columns:
    raise ValueError(
        f"Customer source file is missing columns: {sorted(missing_columns)}"
    )


# Preserve supplied values and add ingestion metadata.
bronze_with_metadata = (
    raw_customers
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)


# File-once ingestion:
# exclude rows belonging to source files that Bronze has already processed.
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