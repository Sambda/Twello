#   Spark
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
#
import json

if __name__ == '__main__':

    KAFKA_TOPIC = "topic_Covid"
    KAFKA_SERVER = "localhost:9092"


    spark = SparkSession \
        .builder \
        .appName("StructredNetworkWordCount") \
        .getOrCreate()

    # Create DataFrame representing the stream of input lines from connection to localhost:9999
    lines = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    # Split the lines into words
    words = lines.select(
        explode(
            split(lines.value, " ")
        ).alias("word")
    )

    # Generate running word count
    wordCounts = words.groupBy("word").count()

    # Start running the query that prints the running counts to the console
    query = wordCounts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()