from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split

def run_click_analytics():
    spark = SparkSession.builder.appName("RealTimeClicks").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Read streaming text input
    df = spark.readStream.format("text").load("click_logs.txt")

    # Parse each line: movie_id,timestamp
    clicks = df.withColumn("value", split(col("value"), ",")) \
               .selectExpr("value[0] as movie_id", "value[1] as timestamp")

    # Aggregate counts per movie
    agg = clicks.groupBy("movie_id").count()

    # Output to console
    query = agg.writeStream \
              .outputMode("complete") \
              .format("console") \
              .trigger(processingTime="5 seconds") \
              .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_click_analytics()

