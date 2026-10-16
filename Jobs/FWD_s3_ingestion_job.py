
import sys
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as f

# Get parameters
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "source_bucket",
    "source_prefix",
    "table_list",
    "target_bucket",
    "target_prefix"
])

# Initialize Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Get parameters
source_bucket = args["source_bucket"].strip()
source_prefix = args["source_prefix"].strip(" /")
target_bucket = args["target_bucket"].strip()
target_prefix = args["target_prefix"].strip(" /")

table_list = args["table_list"].split(",")

# Get today's date
today = datetime.now(timezone.utc)
date_folder = f"{today.day}-{today.month}-{today.year}"

try:
    for table in table_list:

        table = table.strip()
        if not table:
            continue

        # Source path
        source_path = (
            f"s3://{source_bucket}/{source_prefix}/"
            f"{date_folder}/{table}.csv"
        )

        print(f"Reading: {source_path}")

        # Read CSV
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(source_path)
        )

        # Add partition columns
        df = (
            df.withColumn("year", f.lit(today.year))
              .withColumn("month", f.lit(today.month))
              .withColumn("day", f.lit(today.day))
        )

        df.show(5)

        # Target path
        target_path = (
            f"s3://{target_bucket}/{target_prefix}/{table}/"
        )

        print(f"Writing to: {target_path}")

        # Write Parquet with partitions
        (
            df.write
            .mode("overwrite")
            .partitionBy("year", "month", "day")
            .csv(target_path)
        )

        print(f"Completed: {table}")

    job.commit()
    print("Ingestion completed successfully")

except Exception as e:
    print(f"Job failed: {e}")
    raise
