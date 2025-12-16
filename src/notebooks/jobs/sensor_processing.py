from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg, count, min as spark_min, max as spark_max, stddev
import sys
import os

def get_s3_paths():
    if len(sys.argv) > 2:
        input_path = sys.argv[1]
        output_path = sys.argv[2]
    else:
        input_path = os.getenv("INPUT_PATH", "../../../data/raw/data.csv")
        output_path = os.getenv("OUTPUT_PATH", "../../../data/processed/")
    
    return input_path, output_path


def create_spark_session(environment):
    if environment == "local":
        spark = SparkSession.builder.getOrCreate()
    if environment == "remote":
        spark = SparkSession.builder \
                .appName("SensorDataPOC") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                        "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
                .getOrCreate()
    return spark

environment = "remote" if os.getenv("USE_AWS") == "1" else "local"
spark = create_spark_session(environment)

input_path, output_path = get_s3_paths()
print(f"input: {input_path} and output: {output_path}")
    
if not input_path:
    input_path = spark.conf.get("spark.app.input.path", "../../../data/raw/data.csv")
if not output_path:
    output_path = spark.conf.get("spark.app.output.path", "../../../data/processed/")

print(f"=" * 80)
print(f"Starting PySpark Sensor Data Processing Job")
print(f"=" * 80)
print(f"Input Path:  {input_path}")
print(f"Output Path: {output_path}")
print(f"=" * 80)


def main():
    try:
        print("Reading input data...")
        df = spark.read.option("header", True).csv(input_path)
        
        row_count = df.count()
        print(f"✓ Read {row_count} rows")
        
        if row_count == 0:
            print("⚠ Warning: Input data is empty!")
            return
        
        df.printSchema()
        df.show(5)

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

        print("\n" + "=" * 80)
        print("Hourly Aggregated Results:")
        print("=" * 80)
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
            .write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(f"{output_path}/hourly")

        daily_stats_df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(f"{output_path}/daily_stats")

        print(f"✓ Daily statistics written successfully!")
        
        print(f"\n" + "=" * 80)
        print(f"Job completed successfully!")
        print(f"Summary:")
        print(f"  - Input rows: {row_count}")
        print(f"=" * 80)
        
    except Exception as e:
        print(f"✗ Error processing data: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()


