
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as f
from datetime import datetime

## @params: [JOB_NAME, source_path, target_path]
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'source_path', 'target_path']
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Current date
now = datetime.now()
year = now.year
month = f"{now.month:02d}"
day = f"{now.day:02d}"

source_folder = args['source_path']
target_path = args['target_path']

# Silver source paths
Silver_Campaing_customer_path = (
    f"{source_folder}/Silver_Campaing_customer/"
    f"year={year}/month={month}/day={day}/"
)

Silver_policy_path = (
    f"{source_folder}/Silver_policy/"
    f"year={year}/month={month}/day={day}/"
)

# Read CSV from S3
def read_csv_from_s3(path, columns=None):
    df = (
        spark.read
        .format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(path)
    )

    if columns:
        df = df.select(*columns)

    return df


# Required columns
Campaing_customer_columns = [
    "campaign_name",
    "campaign_id",
    "customer_id",
    "lead_status",
    "policy_id",
    "conversion_premium"
]

policy_columns = [
    "policy_id",
    "premium_amount",
    "agent_name"
]

# Read Silver datasets
Silver_Campaing_customer_df = read_csv_from_s3(
    Silver_Campaing_customer_path,
    Campaing_customer_columns
)

Silver_policy_df = read_csv_from_s3(
    Silver_policy_path,
    policy_columns
)

# Join campaign customers with policy data
transformed_df = (
    Silver_Campaing_customer_df.alias("cc")
    .join(
        Silver_policy_df.alias("p"),
        f.col("cc.policy_id") == f.col("p.policy_id"),
        "left"
    )
    .select(
        f.col("cc.campaign_name"),
        f.col("cc.campaign_id"),
        f.col("cc.customer_id"),
        f.col("cc.lead_status"),
        f.col("cc.policy_id"),
        f.col("cc.conversion_premium"),
        f.col("p.premium_amount")
    )
)

# Create conversion indicator
transformed_df = transformed_df.withColumn(
    "policy_converted",
    f.when(
        f.lower(f.trim(f.col("lead_status"))) == "converted",
        1
    ).otherwise(0)
)

# Calculate campaign KPIs
new_df = (
    transformed_df
    .groupBy("campaign_id", "campaign_name")
    .agg(
        f.countDistinct("customer_id").alias("total_customer"),

        f.countDistinct(
            f.when(
                f.col("policy_converted") == 1,
                f.col("policy_id")
            )
        ).alias("policy_converted"),

        f.sum(
            f.when(
                f.col("policy_converted") == 1,
                f.coalesce(f.col("premium_amount"), f.lit(0))
            ).otherwise(0)
        ).alias("Total_premium_amount")
    )
)

# Calculate conversion rate (%)
new_df = new_df.withColumn(
    "conversion_rate",
    f.when(
        f.col("total_customer") > 0,
        f.round(
            f.col("policy_converted") /
            f.col("total_customer") * 100,
            2
        )
    ).otherwise(0)
)

# Calculate average premium per converted policy
new_df = new_df.withColumn(
    "avg_premium_per_conversion",
    f.when(
        f.col("policy_converted") > 0,
        f.round(
            f.col("Total_premium_amount") /
            f.col("policy_converted"),
            2
        )
    ).otherwise(0)
)

# Add partition columns
new_df = (
    new_df
    .withColumn("year", f.lit(year))
    .withColumn("month", f.lit(month))
    .withColumn("day", f.lit(day))
)

# Write Gold KPI output to S3
new_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .format("csv") \
    .option("header", True) \
    .option("partitionOverwriteMode", "dynamic") \
    .save(target_path)

job.commit()
