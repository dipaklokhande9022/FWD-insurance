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

premium_details_path =  f"{source_folder}/premium_details/year={year}/month={month}/day={day}/"
policy_payment_schedule_path =  f"{source_folder}/policy_payment_schedule/year={year}/month={month}/day={day}/"

def read_csv_from_s3(path, columns=["*"]):
    return(
        spark.read.format("csv").option("header",True).option("inferSchema",True).load(path).select(columns)
    )

premium_details_df = read_csv_from_s3(premium_details_path)
policy_payment_schedule_df = read_csv_from_s3(policy_payment_schedule_path)



#transform data as needed 
transformed_df = (
    premium_details_df.alias("pd")\
    .join(
        policy_payment_schedule_df.alias("pps"),
        "policy_id",
        "left"
    )
).select()







transformed_df = transformed_df.withColumn("year",f.lit(year)).withColumn("month",f.lit(month)).withColumn("day",f.lit(day))

transformed_df.write\
    .mode("overwrite")\
    .partitionBy("year","month","day")\
    .format("csv")\
    .option("header",True)\
    .option("partitionOverwriteMode", "dynamic")\
    .save(target_path)
    
job.commit()