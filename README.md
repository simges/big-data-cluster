# big-data-cluster
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
-run demo.py
