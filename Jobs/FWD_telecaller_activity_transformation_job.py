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
telecaller_path = f"{source_folder}/telecaller/year={year}/month={month}/day=16/"
telecaller_activity_path = f"{source_folder}/telecaller_activity/year={year}/month={month}/day=16/"

# Define a function to read CSV files from S3 with specified columns
def read_csv_from_s3(path, columns=["*"]):
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(path)
        .select(columns)
    )

# Read the source data from S3 for telecaller, and telecaller_activity
telecaller_df = read_csv_from_s3(telecaller_path)
telecaller_activity_df = read_csv_from_s3(telecaller_activity_path)

# Transform the data as needed
transformed_df = (
        telecaller_df.alias("t")
            .join(
                telecaller_activity_df.alias("ta"), 
                "telecaller_id", 
                "left"
            ).select(
                "t.telecaller_id",
                "t.telecaller_name",
                "t.region",
                "t.supervisor_id",
                "t.telecaller_status",
                "ta.customer_id",
                "ta.policy_id",
                "ta.campaign_id",
                "ta.outcome"
            ).withColumn(
                "outcome", 
                    F.trim(F.upper(F.col("ta.outcome")))
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