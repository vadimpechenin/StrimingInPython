from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaSparkTest") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "car_stream") \
    .load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .writeStream \
  .outputMode("append") \
  .format("console") \
  .start() \
  .awaitTermination()