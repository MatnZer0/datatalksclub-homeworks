import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F

pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("GreenTripsConsumer") \
    .config("spark.jars.packages", kafka_jar_package) \
    .getOrCreate()

green_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda-1:29092") \
    .option("subscribe", "green-trips2") \
    .option("startingOffsets", "earliest") \
    .load()

schema = types.StructType() \
    .add("VendorID", types.IntegerType()) \
    .add("lpep_pickup_datetime", types.StringType()) \
    .add("lpep_dropoff_datetime", types.StringType()) \
    .add("PULocationID", types.IntegerType()) \
    .add("DOLocationID", types.IntegerType()) \
    .add("passenger_count", types.DoubleType()) \
    .add("trip_distance", types.DoubleType()) \
    .add("tip_amount", types.DoubleType())

popular_destinations = green_stream \
    .select(F.from_json(F.col("value").cast('STRING'), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", F.current_timestamp()) \
    .groupBy(F.window(F.col("timestamp"), "5 minutes"), "DOLocationID") \
    .count() \
    .orderBy(F.desc("count"))  # Ordering in descending order

query = popular_destinations \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()