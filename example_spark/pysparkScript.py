from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Example PySpark Script") \
    .getOrCreate()

# Read a CSV file into a DataFrame
data_df = spark.read.csv("data/features_demo.csv", header=True, inferSchema=True)

# Show the first 5 rows of the DataFrame
data_df.show(5)

# Print the schema of the DataFrame
data_df.printSchema()

# Perform a simple transformation
transformed_df = data_df.select("feature_1", "feature_2").where(data_df["feature_1"] > 0.005)

# Show the result of the transformation
transformed_df.show(5)

# Stop the Spark session
spark.stop()
