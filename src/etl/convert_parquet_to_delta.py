from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("ConvertParquetToDelta") \
        .config("spark.hadoop.fs.s3a.access.key", "<ACCESS_KEY>") \
        .config("spark.hadoop.fs.s3a.secret.key", "<SECRET_KEY>") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://<MINIO_ENDPOINT>") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

    df_parquet = spark.read.parquet("s3a://<bucket>/<path-to-your-output-file>.parquet")
    df_parquet.write.format("delta").save("/path/to/delta/table")

if __name__ == "__main__":
    main()
