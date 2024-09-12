from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("ConvertCSVToParquet") \
        .config("spark.hadoop.fs.s3a.access.key", "<ACCESS_KEY>") \
        .config("spark.hadoop.fs.s3a.secret.key", "<SECRET_KEY>") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://<MINIO_ENDPOINT>") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

    df = spark.read.csv("s3a://<bucket>/<path-to-your-file>.csv", header=True, inferSchema=True)
    df.write.parquet("s3a://<bucket>/<path-to-your-output-file>.parquet", mode='overwrite')

if __name__ == "__main__":
    main()
