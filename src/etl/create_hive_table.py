from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("CreateHiveTable") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("""
        CREATE TABLE delta_table
        USING DELTA
        LOCATION '/path/to/delta/table'
    """)

if __name__ == "__main__":
    main()
