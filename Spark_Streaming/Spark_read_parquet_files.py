import pyspark
#sqlContext = org.apache.spark.sql.SQLContext(sc)
#df = spark.read.parquet("parc/part-00000-7426f3a3-7926-4584-af7b-700912438bd2-c000.snappy.parquet")
#df = pyspark.sql("SELECT * FROM parquet.` parc/part-00000-7426f3a3-7926-4584-af7b-700912438bd2-c000.snappy.parquet`")
#df.show()
#parquetFile = spark.read.parquet("people.parquet")

# Parquet files can also be used to create a temporary view and then used in SQL statements.
#parquetFile.createOrReplaceTempView("parquetFile")
#teenagers = spark.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
#teenagers.show()


import pandas as pd
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import pyspark.sql.types
import datetime

import time

from pyspark.sql import SparkSession
# initialise sparkContext
spark = SparkSession.builder \
    .master('local') \
    .appName('TwitterSentimentAnalysis') \
    .config('spark.executor.memory', '5gb') \
    .config("spark.cores.max", "6") \
    .getOrCreate()

sc = spark.sparkContext

# using SQLContext to read parquet file
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# to read parquet file
df = sqlContext.read.parquet('parc/')
df.show()