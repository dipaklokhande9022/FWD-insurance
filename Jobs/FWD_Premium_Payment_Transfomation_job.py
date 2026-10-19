import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as f
from datetime import datetime

## @params: [JOB_NAME, source_path, target_path]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_path', 'target_path'])

sc = SparkContext()
glueContext = GlueContext(sc)

# --- CRITICAL CONFIGURATION FOR DYNAMIC OVERWRITES ON S3 ---
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get current date and format months/days with zero-padding
now = datetime.now()
year = str(now.year)
month = f"{now.month:02d}"
day = f"{now.day:02d}"

source_folder = args.get("source_path")
target_path = args.get("target_path")

premium_details_path = f"{source_folder}/premium_details/year={year}/month={month}/day={day}/"
policy_payment_schedule_path = f"{source_folder}/policy_payment_schedule/year={year}/month={month}/day={day}/"

def read_csv_from_s3(path, columns=["*"]):
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(path)
        .select(columns)
    )

premium_details_df = read_csv_from_s3(premium_details_path)
policy_payment_schedule_df = read_csv_from_s3(policy_payment_schedule_path)

# --- Transform Data ---

# FIXED: Drop the duplicate timestamp column from the schedule table before joining
pps_cleaned_df = policy_payment_schedule_df.drop("api_updated_timestamp")

transformed_df = premium_details_df.alias("pd") \
    .join(
        pps_cleaned_df.alias("pps"),  # Using the cleaned dataframe here
        "policy_id",
        "left"
    ).select(
        "pd.*",
        "pps.scheduled_amount",
        "pps.grace_period_days",
        "pps.lapse_date",
        "pps.schedule_status"
    ) \
    .withColumn("Overdue_days", f.datediff(f.current_date(), f.to_date(f.col("pd.due_date"), "yyyy-MM-dd")))

# Add partition metadata columns
transformed_df = transformed_df \
    .withColumn("year", f.lit(year)) \
    .withColumn("month", f.lit(month)) \
    .withColumn("day", f.lit(day))

# --- Write Out Data ---
transformed_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .format("csv") \
    .option("header", True) \
    .option("partitionOverwriteMode", "dynamic") \
    .save(target_path)

job.commit()


 
