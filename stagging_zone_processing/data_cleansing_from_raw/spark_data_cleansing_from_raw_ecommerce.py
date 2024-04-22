import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, BooleanType, TimestampType
from pyspark.sql.functions import col, to_timestamp

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_environment_variables():
    """ Check for all necessary environment variables """
    required_vars = [
        'S3_ENDPOINT', 'SOURCE_BUCKET', 'DESTINATION_BUCKET',
        'SOURCE_ROLE_ARN', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
        'SPARK_MASTER_URL', 'STS_ENDPOINT', 'STS_REGION', 'SESSION_DURATION'
    ]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logging.error("Missing environment variables:")
        for var in missing_vars:
            logging.error(var)
        sys.exit(1)

def main():
    """ Main function to process data using Spark with AWS IAM role assumption """
    check_environment_variables()
    spark_master_url = os.getenv('SPARK_MASTER_URL', 'spark://localhost:7077')

    # Create a Spark session configured for AWS IAM role assumption
    spark = SparkSession.builder \
        .appName("Data Processing with IAM Role Assumption") \
        .master(spark_master_url) \
        .config("spark.driver.host", "10.88.0.6") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv('S3_ENDPOINT')) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID')) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.assumed.role.arn", os.getenv('SOURCE_ROLE_ARN')) \
        .config("spark.hadoop.fs.s3a.assumed.role.sts.endpoint", os.getenv('STS_ENDPOINT')) \
        .config("spark.hadoop.fs.s3a.assumed.role.sts.endpoint.region", os.getenv('STS_REGION')) \
        .config("spark.hadoop.fs.s3a.assumed.role.session.duration", os.getenv('SESSION_DURATION')) \
        .config("spark.hadoop.fs.s3a.assumed.role.session.name", "sparkSession") \
        .config("spark.hadoop.fs.s3a.assumed.role.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .getOrCreate()

    # Define schema for browsing data
    browsing_schema = StructType([
        StructField("ip", StringType(), True),
        StructField("ts", StringType(), True),
        StructField("tz", StringType(), True),
        StructField("verb", StringType(), True),
        StructField("resource_type", StringType(), True),
        StructField("resource_fk", StringType(), True),
        StructField("response", IntegerType(), True),
        StructField("browser", StringType(), True),
        StructField("os", StringType(), True),
        StructField("customer", IntegerType(), True),
        StructField("d_day_name", StringType(), True),
        StructField("i_current_price", DoubleType(), True),
        StructField("i_category", StringType(), True),
        StructField("i_description", StringType(), True),
        StructField("c_preferred_cust_flag", BooleanType(), True),
        StructField("ds", DateType(), True)
    ])

    # Load and cleanse browsing data
    browsing_df = spark.read.schema(browsing_schema).csv(f"s3a://{os.getenv('SOURCE_BUCKET')}/browsing/*.csv")
    # Convert 'ts' to a Timestamp type within the DataFrame
    browsing_df = browsing_df.withColumn("ts", to_timestamp(col("ts"), "yyyy-MM-dd HH:mm:ss"))

    browsing_cleaned = browsing_df.dropDuplicates()

    # Write cleaned and partitioned browsing data to the STAGING zone in Parquet format
    browsing_cleaned.write.partitionBy("ds").mode("overwrite").parquet(f"s3a://{os.getenv('DESTINATION_BUCKET')}/browsing/")

    spark.stop()

if __name__ == "__main__":
    main()
