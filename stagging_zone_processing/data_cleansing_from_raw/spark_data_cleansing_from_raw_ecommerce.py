
import os
import sys
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType

def get_jwt_token(provider_url, client_id, client_secret):
    ''' Obtain JWT token from OIDC provider. '''
    username = os.getenv('OIDC_USERNAME')
    password = os.getenv('OIDC_PASSWORD')
    token_endpoint = f"{provider_url}/token"
    payload = {
        'grant_type': 'password',
        'client_id': client_id,
        'client_secret': client_secret,
        'username': username,
        'password': password
    }
    try:
        response = requests.post(token_endpoint, data=payload)
        response.raise_for_status()
        return response.json()['access_token']
    except Exception as e:
        print(f"Error obtaining JWT token: {e}")
        return None

def check_environment_variables():
    required_vars = [
        'S3_ENDPOINT', 'SOURCE_BUCKET', 'DESTINATION_BUCKET',
        'SOURCE_ROLE_ARN', 'DESTINATION_ROLE_ARN',
        'OIDC_PROVIDER_URL', 'OIDC_CLIENT_ID', 'OIDC_CLIENT_SECRET',
        'OIDC_USERNAME', 'OIDC_PASSWORD'
    ]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print("Missing environment variables:")
        for var in missing_vars:
            print(var)
        print("\nPlease set the environment variables before running the script.")
        print("Example:")
        print("export S3_ENDPOINT='<your-s3-endpoint>'")
        print("export SOURCE_BUCKET='<your-source-bucket>'")
        print("export DESTINATION_BUCKET='<your-destination-bucket>'")
        print("... other variables ...")
        sys.exit(1)

def main():
    check_environment_variables()
    provider_url = os.getenv('OIDC_PROVIDER_URL')
    client_id = os.getenv('OIDC_CLIENT_ID')
    client_secret = os.getenv('OIDC_CLIENT_SECRET')
    source_role_arn = os.getenv('SOURCE_ROLE_ARN')
    destination_role_arn = os.getenv('DESTINATION_ROLE_ARN')
    source_bucket = os.getenv('SOURCE_BUCKET')
    destination_bucket = os.getenv('DESTINATION_BUCKET')
    s3_endpoint = os.getenv('S3_ENDPOINT')

    jwt_token = get_jwt_token(provider_url, client_id, client_secret)
    
    spark = SparkSession.builder.appName("Data Processing with JWT and IAM Roles") \
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider") \
        .config("spark.hadoop.fs.s3a.assumed.role.arn", source_role_arn) \
        .config("spark.hadoop.fs.s3a.assumed.role.session.name", "spark-session") \
        .config("spark.hadoop.fs.s3a.assumed.role.token.providers", "org.apache.hadoop.fs.s3a.auth.CustomSessionTokenProvider") \
        .config("spark.hadoop.fs.s3a.extra.AWS_SESSION_TOKEN", jwt_token) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .getOrCreate()

    # Schema for browsing data
    browsingSchema = StructType([
        StructField("session_id", StringType(), True),
        StructField("client_id", IntegerType(), True),
        StructField("page_views", IntegerType(), True),
        StructField("ts", DateType(), True)  # Assuming 'ts' is a timestamp
    ])

    # Load and cleanse browsing data
    browsing_df = spark.read.schema(browsingSchema).csv(f"s3a://{source_bucket}/browsing/*.csv")
    browsing_cleaned = browsing_df.dropDuplicates().na.fill({"page_views": 0})

    # Write cleaned and partitioned browsing data to the STAGING zone in Parquet format
    browsing_cleaned.write.partitionBy("ts").mode("overwrite").parquet(f"s3a://{destination_bucket}/browsing/")

    spark.stop()

if __name__ == "__main__":
    main()
