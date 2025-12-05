from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg
import os
from dotenv import load_dotenv

load_dotenv() 

def main():
    spark = SparkSession.builder \
        .appName("SensorDataPOC") \
        .getOrCreate()

    input_csv = os.environ.get("INPUT_PATH", "data/data.csv")
    output_csv = os.environ.get("OUTPUT_PATH", "data/output.csv")

    df = spark.read.option("header", True).csv(input_csv)

    df = df.withColumn("temperature_c", col("temperature_c").cast("double")) \
           .withColumn("humidity_pct", col("humidity_pct").cast("double")) \
           .withColumn("co2_ppm", col("co2_ppm").cast("double"))

    df = df.withColumn("hour", hour(col("measurement_time")))

    agg_df = df.groupBy("location", "hour").agg(
        avg("temperature_c").alias("avg_temperature_c"),
        avg("co2_ppm").alias("avg_co2_ppm")
    )

    agg_df.show()
    agg_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_csv)

    print(f"Output written to {output_csv}")
    spark.stop()

if __name__ == "__main__":
    main()
