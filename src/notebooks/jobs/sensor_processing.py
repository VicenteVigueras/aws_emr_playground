from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg, count, min as spark_min, max as spark_max, stddev
import sys
import os
from dotenv import load_dotenv


load_dotenv()  

USE_AWS = os.getenv("USE_AWS", "0") == "1"
environment = "remote" if USE_AWS else "local"

ENV_INPUT = os.getenv("INPUT_PATH")
ENV_OUTPUT = os.getenv("OUTPUT_PATH")

if not ENV_INPUT or not ENV_OUTPUT:
    raise ValueError("INPUT_PATH and OUTPUT_PATH must be defined in your .env file")


def get_paths():
    if len(sys.argv) > 2:
        return sys.argv[1], sys.argv[2]
    return ENV_INPUT, ENV_OUTPUT

input_path, output_path = get_paths()

print(f"[INFO] Environment: {environment}")
print(f"[INFO] Input Path: {input_path}")
print(f"[INFO] Output Path: {output_path}")


def create_spark_session(env):
    builder = SparkSession.builder.appName("SensorDataPOC")
    if env == "remote":
        builder = builder \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                    "com.amazonaws.auth.InstanceProfileCredentialsProvider")
    return builder.getOrCreate()

spark = create_spark_session(environment)


def main():
    try:
        print("Reading input data...")
        df = spark.read.option("header", True).csv(input_path)
        row_count = df.count()
        print(f"✓ Read {row_count} rows")
        if row_count == 0:
            print("⚠ Warning: Input data is empty!")
            return

        df = df.withColumn("temperature_c", col("temperature_c").cast("double")) \
               .withColumn("humidity_pct", col("humidity_pct").cast("double")) \
               .withColumn("co2_ppm", col("co2_ppm").cast("double"))

        df = df.withColumn("hour", hour(col("measurement_time")))

        hourly_agg_df = df.groupBy("location", "hour").agg(
            avg("temperature_c").alias("avg_temperature_c"),
            avg("humidity_pct").alias("avg_humidity_pct"),
            avg("co2_ppm").alias("avg_co2_ppm"),
            count("*").alias("measurement_count")
        ).orderBy("location", "hour")
        hourly_agg_df.show(20, truncate=False)

        daily_stats_df = df.groupBy("location").agg(
            count("*").alias("total_measurements"),
            avg("temperature_c").alias("daily_avg_temperature_c"),
            spark_min("temperature_c").alias("min_temperature_c"),
            spark_max("temperature_c").alias("max_temperature_c"),
            stddev("temperature_c").alias("temp_std_dev"),
            avg("humidity_pct").alias("daily_avg_humidity_pct"),
            avg("co2_ppm").alias("daily_avg_co2_ppm"),
            spark_max("co2_ppm").alias("max_co2_ppm")
        ).orderBy("location")

        daily_stats_df = daily_stats_df.withColumn(
            "temp_range_c",
            col("max_temperature_c") - col("min_temperature_c")
        )
        daily_stats_df.show(truncate=False)

        hourly_agg_df.coalesce(1) \
            .write.mode("overwrite") \
            .option("header", True) \
            .csv(f"{output_path}/hourly")
        
        daily_stats_df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(f"{output_path}/daily_stats")
        print(f"✓ Daily statistics written successfully!")

    except Exception as e:
        print(f"✗ Error processing data: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    main()
