from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg
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

        print("Aggregating data by location and hour...")
        agg_df = df.groupBy("location", "hour").agg(
            avg("temperature_c").alias("avg_temperature_c"),
            avg("humidity_pct").alias("avg_humidity_pct"),
            avg("co2_ppm").alias("avg_co2_ppm")
        ).orderBy("location", "hour")

        print("\n" + "=" * 80)
        print("Aggregated Results:")
        print("=" * 80)
        agg_df.show(20, truncate=False)
        
        agg_count = agg_df.count()
        print(f"✓ Aggregated to {agg_count} rows")

        print(f"\nWriting output to {output_path}...")
        agg_df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(output_path)

        print(f"✓ Output written successfully!")
        print(f"=" * 80)
        print(f"Job completed successfully!")
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