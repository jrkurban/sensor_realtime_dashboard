from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from elasticsearch import ElasticSearch, helpers

spark = (SparkSession.builder.appName("Kafka to Elasticsearch")
                     .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:7.12.1,"
                                                     "org.apache.spark:spark-sql-kafka-0_10_2.12:3.1.1")
                     .getOrCreate())

# read data from kafka source
line = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092, localhost:9292") \
    .option("subscribe", "office-input") \
    .load()