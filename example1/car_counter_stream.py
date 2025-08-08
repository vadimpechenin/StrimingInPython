from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import StructType, StringType, DoubleType

# Создание SparkSession
spark = SparkSession.builder \
    .appName("CarTypeStreamingCounter") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Определение схемы входного JSON
schema = StructType() \
    .add("type", StringType()) \
    .add("timestamp", DoubleType())

# Подключение к Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "car_stream") \
    .load()

# Преобразование value из байт в JSON
df_parsed = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Группировка по типу автомобиля и оконному интервалу
df_grouped = df_parsed.withColumn("event_time", col("timestamp").cast("timestamp")) \
    .groupBy(
        window(col("event_time"), "10 seconds"),
        col("type")
    ).agg(count("*").alias("count"))

# Вывод результата в консоль
query = df_grouped.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
