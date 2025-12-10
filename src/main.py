from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg, count, min as spark_min, max as spark_max, stddev
from pyspark.sql.window import Window
import sys
import os

def get_s3_paths():
    """
    Get S3 paths from Spark configuration or environment
    """
    if len(sys.argv) > 2:
        input_path = sys.argv[1]
        output_path = sys.argv[2]
    else:
        input_path = os.getenv("INPUT_PATH", "")
        output_path = os.getenv("OUTPUT_PATH", "")
    
    return input_path, output_path

def main():
    """
    PySpark job to process sensor data
    Reads from S3, aggregates by location and hour, writes back to S3
    
    Enhanced with:
    - Daily statistics per location
    - Temperature range calculations
    - Measurement counts
    - Standard deviation for data quality monitoring
    """
    spark = SparkSession.builder \
        .appName("SensorDataPOC") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
        .getOrCreate()

    input_path, output_path = get_s3_paths()
    
    if not input_path:
        input_path = spark.conf.get("spark.app.input.path", "")
    if not output_path:
        output_path = spark.conf.get("spark.app.output.path", "")
    
    print(f"=" * 80)
    print(f"Starting PySpark Sensor Data Processing Job")
    print(f"=" * 80)
    print(f"Input Path:  {input_path}")
    print(f"Output Path: {output_path}")
    print(f"=" * 80)

    try:
        print("Reading input data...")
        df = spark.read.option("header", True).csv(input_path)
        
        row_count = df.count()
        print(f"✓ Read {row_count} rows")
        
        if row_count == 0:
            print("⚠ Warning: Input data is empty!")
            return
        
        df.printSchema()
        print("\nSample data:")
        df.show(5)

        print("\nCasting numeric columns...")
        df = df.withColumn("temperature_c", col("temperature_c").cast("double")) \
               .withColumn("humidity_pct", col("humidity_pct").cast("double")) \
               .withColumn("co2_ppm", col("co2_ppm").cast("double"))

        print("Extracting hour from measurement_time...")
        df = df.withColumn("hour", hour(col("measurement_time")))

        # Original aggregation by location and hour
        print("Aggregating data by location and hour...")
        hourly_agg_df = df.groupBy("location", "hour").agg(
            avg("temperature_c").alias("avg_temperature_c"),
            avg("humidity_pct").alias("avg_humidity_pct"),
            avg("co2_ppm").alias("avg_co2_ppm"),
            count("*").alias("measurement_count")  # NEW: Count of measurements per hour
        ).orderBy("location", "hour")

        print("\n" + "=" * 80)
        print("Hourly Aggregated Results:")
        print("=" * 80)
        hourly_agg_df.show(20, truncate=False)
        
        hourly_count = hourly_agg_df.count()
        print(f"✓ Aggregated to {hourly_count} hourly records")

        # NEW: Additional daily statistics per location
        print("\n" + "=" * 80)
        print("Computing Daily Statistics by Location...")
        print("=" * 80)
        
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
        
        # Add temperature range calculation
        daily_stats_df = daily_stats_df.withColumn(
            "temp_range_c",
            col("max_temperature_c") - col("min_temperature_c")
        )
        
        print("\nDaily Statistics by Location:")
        daily_stats_df.show(truncate=False)
        
        daily_count = daily_stats_df.count()
        print(f"✓ Generated statistics for {daily_count} locations")

        # Write both outputs
        print(f"\nWriting hourly aggregations to {output_path}/hourly/...")
        hourly_agg_df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(f"{output_path}/hourly")

        print(f"✓ Hourly output written successfully!")
        
        print(f"\nWriting daily statistics to {output_path}/daily_stats/...")
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
        print(f"  - Hourly aggregations: {hourly_count}")
        print(f"  - Daily statistics: {daily_count}")
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