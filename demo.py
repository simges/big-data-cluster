# ./pyspark --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2

# 1- HADOOP - SPARK CLUSTER SETUP

# WRITE TO HDFS
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkHadoopParquetDemo") \
    .getOrCreate()

data = [("apple",), ("banana",), ("apple",), ("orange",), ("banana",), ("apple",)]

df = spark.createDataFrame(data, ["fruit"])

# Write as Parquet to HDFS
df.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/user/spark/fruits_parquet2")

spark.stop()

# READ FROM HDFS
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadParquetDemo") \
    .getOrCreate()

df = spark.read.parquet("hdfs://namenode:9000/user/spark/fruits_parquet2")

df.show()


# 2- KAFKA - SPARK

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaStreaming") \
    .getOrCreate()


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "test-topic") \
    .load()

messages = df.selectExpr("CAST(value AS STRING)")

query = messages.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()


# 2- KAFKA - SPARK - HDFS

# Kafka → Spark Streaming → HDFS (Parquet storage)

# # # Kafka (events)
# # #    ↓
# # # Spark Structured Streaming
# # #    ↓
# # # Transformations
# # #    ↓
# # # HDFS (Parquet files)


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("KafkaToHDFSParquetPipeline") \
    .getOrCreate()

# 1. Read streaming data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "test-topic") \
    .load()

# 2. Convert Kafka binary value → string
messages = df.select(col("value").cast("string").alias("fruit"))

# 3. (Optional) transformation (example: simple dedup logic)
cleaned = messages.dropDuplicates()

# 4. Write stream to HDFS as Parquet
query = cleaned.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "hdfs://namenode:9000/user/spark/kafka_test2") \
    .option("checkpointLocation", "hdfs://namenode:9000/user/spark/checkpoints/kafka_pipeline2") \
    .start()

query.awaitTermination()


# -----kafka-------------
# ./pyspark --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2

# # # apple
# # # banana
# # # apple
# # # orange
# # # banana
# # # apple


# -------spark-----------------

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadHDFSParquet") \
    .getOrCreate()

df = spark.read.parquet("hdfs://namenode:9000/user/spark/kafka_test")

df.show()

