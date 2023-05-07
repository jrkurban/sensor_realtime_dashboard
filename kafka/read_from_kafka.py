from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Read From Kafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()

line = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092,localhost:9292") \
  .option("subscribe", "topic1") \
  .load()

# deserialize
line2 = line.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition")


# Parsing
# SepalLengthCm,SepalWidthCm,PetalLengthCm,PetalWidthCm,Species
line3 = line2.withColumn("SepalLengthCm", F.split(F.col("value"), ",")[0].cast(FloatType())) \
        .withColumn("SepalWidthCm", F.split(F.col("value"), ",")[1].cast(FloatType())) \
        .withColumn("PetalLengthCm", F.split(F.col("value"), ",")[2].cast(FloatType())) \
        .withColumn("PetalWidthCm", F.split(F.col("value"), ",")[3].cast(FloatType())) \
        .withColumn("Species", F.split(F.col("value"), ",")[4]) \
        .drop("value")

# operation
line4 = line3.withColumn("sum_sepal", (line3.SepalLengthCm + line3.SepalWidthCm)) \
        .withColumn("sum_petal", (line3.PetalLengthCm + line3.PetalWidthCm))


line5 = line4.withColumn("value", F.concat(line4.Species, F.lit(","), line4.sum_sepal, F.lit(","), line4.sum_petal))



checkpointDir = "file:///tmp/streaming/writefrom_kafka"

# streamingQuery = (line4.writeStream.format("console")
# .outputMode("append")
#                   .trigger(processingTime="4 second")
#                   .option("numRows", 4)
#                   .option("truncate", False)
#                   .option("checkpointLocation", checkpointDir)
#                   .start()
#  )

streamingQuery = line5 \
    .selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092,localhost:9292") \
    .option("topic", "topic2") \
    .option("checkpointLocation", checkpointDir) \
    .start()


streamingQuery.awaitTermination()