# Big Data Cluster
Deploying a Big Data Cluster

Kafka (events)
   ↓
Spark Structured Streaming
   ↓
Transformations
   ↓
HDFS (Parquet files)

# Create Common Network
docker network create bigdata

# Run Docker Containers
cd hadoop
docker compose up 
cd ..

cd spark
docker compose up 
cd..

cd kafka
docker compose up 
cd ..

# Create Kafka Topic and Produce Data
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic test-topic \
  --bootstrap-server kafka:9092 \
  --partitions 1 \
  --replication-factor 1

docker exec -it kafka /opt/kafka/bin/kafka-topics.sh   --list  --bootstrap-server kafka:9092

docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh   --topic test-topic   --bootstrap-server 0.0.0.0:9092
apple
banana
apple
orange
banana
apple

# Kafka Consumer Optional
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh   --topic test-topic   --from-beginning   --bootstrap-server 0.0.0.0:9092

# Run Spark Job
./pyspark --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2


# WordCount with different Spark APIs

# PYSPARK
./pyspark --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2

&&

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()

text = spark.read.text("/user/spark/logs/logfile1.txt")

from pyspark.sql.functions import explode, split

words = text.select(explode(split(text.value, " ")).alias("word"))

word_counts = words.groupBy("word").count()

word_counts.show()

# JAVA Spark

SparkSession spark = SparkSession.builder()
    .appName("WordCount")
    .getOrCreate();

Dataset<Row> text = spark.read().text("/user/spark/logs/logfile1.txt");

Dataset<Row> words = text.select(
    functions.explode(
        functions.split(text.col("value"), " ")
    ).alias("word")
);

Dataset<Row> wordCounts = words.groupBy("word").count();

wordCounts.show();

mvn clean package 

./spark-submit \
  --class WordCount \
  --master local[*] \
  ../work-dir/spark-java-app-1.0-jar-with-dependencies.jar

# Spark SQL

./spark-sql

CREATE OR REPLACE TEMP VIEW text_table
USING text
OPTIONS (path "/user/spark/logs/logfile1.txt");

SELECT word, COUNT(*) AS count
FROM (
    SELECT explode(split(value, ' ')) AS word
    FROM text_table
)
GROUP BY word;


0- WORD COUNT EXAMPLE
1- HADOOP - SPARK CLUSTER SETUP
2- KAFKA - SPARK
3- KAFKA - SPARK - HDFS
   Parquet File Format
4- MULTI-NODE HDFS (Fault tolerance)
5- Capacity Scheduling
