import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
import pyspark.sql.functions as F

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_path', 'target_path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
# Get the current date and format it as year, month, and day
now = datetime.now()
year = now.year
month = f"{now.month:02d}"
day = f"{now.day:02d}"


# Get the source and target paths from the job arguments
source_folder = args.get("source_path")
target_path = args.get("target_path")

# Construct the actual source path based on the current date
campaign_path = f"{source_folder}/campaign/year={year}/month={month}/day={day}/"
campaign_customer_path = f"{source_folder}/campaign_customer/year={year}/month={month}/day={day}/"

# Define a function to read CSV files from S3 with specified columns
def read_csv_from_s3(path, columns=["*"]):
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(path)
        .select(columns)
    )

# Read the source data from S3 for campaign, and campaign_customer
campaign_df = read_csv_from_s3(campaign_path)
campaign_customer_df = read_csv_from_s3(campaign_customer_path)

# Transform the data as needed
transformed_df = (
        campaign_df.alias("c")
            .join(
                campaign_customer_df.alias("cc"), 
                "campaign_id", 
                "left"
            ).select(
                "c.campaign_name",
                "c.campaign_type",
                "c.campaign_id",
                "cc.customer_id",
                "cc.lead_status",
                "cc.policy_id",
                "cc.conversion_premium"
            ).withColumn(
                "lead_status", 
                    F.trim(F.lower(F.col("lead_status")))
            ).withColumn(
                "conversion_premium", 
                    F.when(
                        F.col("conversion_premium").isNull(), 
                        0
                    ).otherwise(
                        F.col("conversion_premium")
                    )
            )
)

# Add year, month, and day columns to the DataFrame for partitioning
transformed_df = (
        transformed_df.withColumn("year", F.lit(year))
            .withColumn("month", F.lit(month))
            .withColumn("day", F.lit(day))
)

# Write the transformed data to the target S3 location with partitioning
(
    transformed_df.write
            .mode("overwrite")
            .partitionBy("year", "month", "day")
            .format("csv")
            .option("header", True)
            .save(target_path)
)

job.commit()