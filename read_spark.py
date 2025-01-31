from pyspark.sql import SparkSession

def read_transactions_from_minio():
    # Create Spark session with MinIO configuration
    spark = SparkSession.builder \
        .appName("ReadMinioParquet") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("fs.s3a.endpoint", "http://localhost:9000") \
        .config("fs.s3a.access.key", "minio") \
        .config("fs.s3a.secret.key", "minio123") \
        .config("fs.s3a.path.style.access", "true") \
        .getOrCreate()

    # Read Parquet files from MinIO
    transactions_df = spark.read.parquet("s3a://warehouse/transactions")

    return transactions_df


transactions = read_transactions_from_minio()
transactions.show()
transactions.printSchema()