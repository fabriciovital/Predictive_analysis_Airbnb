from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("LoadCSVToSpark") \
        .config("spark.hadoop.fs.s3a.access.key", "<MINIO_ACCESS_KEY>") \
        .config("spark.hadoop.fs.s3a.secret.key", "<MINIO_SECRET_KEY>") \
        .config("spark.hadoop.fs.s3a.endpoint", "MINIO_ENDPOINT") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

    df = spark.read.csv("s3a://airbnb-datalake/<path-to-your-file>.csv", header=True, inferSchema=True)
    df.show()

if __name__ == "__main__":
    main()