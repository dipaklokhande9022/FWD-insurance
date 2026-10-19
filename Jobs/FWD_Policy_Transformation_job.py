import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as f
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','source_path', 'target_path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

now = datetime.now()
year = now.year
month = f"{now.month:02d}"
day = f"{now.day:02d}"

source_folder = args.get("source_path")
target_path = args.get("target_path")

policy_path =  f"{source_folder}/policy/year={year}/month={month}/day={day}"
insurance_product_path =  f"{source_folder}/insurance_product/year={year}/month={month}/day={day}/"
agent_path =  f"{source_folder}/agent/year={year}/month={month}/day={day}/"

def read_csv_from_s3(path, columns=["*"]):
    return(
        spark.read.format("csv").option("header",True).option("inferSchema",True).load(path).select(columns)
    )

policy_df = read_csv_from_s3(policy_path, ["policy_id", "customer_id", "product_id", "agent_id", "policy_start_date", "premium_amount", "policy_status"])
insurance_product_df = read_csv_from_s3(insurance_product_path, ["product_id", "product_name", "product_type", "product_category"])
agent_df = read_csv_from_s3(agent_path,["agent_id", "agent_name", "region"])

agent_df = agent_df.select("agent_id", "agent_name", "region")
insurance_product_df = insurance_product_df.select("product_id", "product_name", "product_type", "product_category" )
policy_df = policy_df.select("policy_id", "customer_id", "product_id", "agent_id", "policy_start_date", "premium_amount", "policy_status")

#transform data as needed 

transformed_df = policy_df.alias("p") \
    .join(insurance_product_df.alias("ip"), "product_id", "left") \
    .join(agent_df.alias("a"), "agent_id", "left") \
    .select(
        "policy_id",
        "customer_id",
        "product_id",          # FIXED: Unified key (No "p." table prefix)
        "policy_start_date",
        "premium_amount",
        "p.policy_status",     
        "ip.product_name",
        "ip.product_type",
        "ip.product_category",
        "agent_id",            # FIXED: Unified key (No "p." table prefix)
        "a.agent_name",
        "a.region"
    ) \
    .withColumn("policy_status", f.upper(f.col("policy_status"))) \
    .withColumn("region", f.upper(f.col("region")))

transformed_df = transformed_df.withColumn("year",f.lit(year)).withColumn("month",f.lit(month)).withColumn("day",f.lit(day))

transformed_df.write\
    .mode("overwrite")\
    .partitionBy("year","month","day")\
    .format("csv")\
    .option("header",True)\
    .option("partitionOverwriteMode", "dynamic")\
    .save(target_path)
    
job.commit()