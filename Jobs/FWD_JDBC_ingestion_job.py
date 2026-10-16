
import sys
import socket
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as f

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Replace with the exact endpoint from RDS
host = "YOUR_ACTUAL_RDS_ENDPOINT"
port = 3306

# Test network connection
try:
    conn = socket.create_connection((host, port), timeout=10)
    conn.close()
    print("RDS network connection successful")
except Exception as e:
    print(f"RDS network connection failed: {e}")
    raise

# Read MySQL table
jdbc_url = f"jdbc:mysql://{host}:3306/fwd_insurance"

df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "customer")
    .option("user", "YOUR_DB_USERNAME")
    .option("password", "YOUR_DB_PASSWORD")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .load()
)

# Add date columns
now = datetime.now()

df = (
    df.withColumn("year", f.lit(now.year))
      .withColumn("month", f.lit(now.month))
      .withColumn("day", f.lit(now.day))
)

target_path = (
    "s3://fwd-insurance-project/FWD/Bronze/customer_jdbc/"
)

# Write partitioned CSV data
(
    df.write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .option("header", "true")
    .csv(target_path)
)

job.commit()
print("Customer JDBC ingestion completed")
