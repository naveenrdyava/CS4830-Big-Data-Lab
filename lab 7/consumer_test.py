from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession.builder.appName("SparkConsumerTest").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.168.0.3:9092") \
    .option("startingOffsets", "earliest") \
    .option("subscribe", "lab7_topic") \
    .load()

'''words = df.select(
    explode(
        split(df.value, " ")
    ).alias("word")
)'''

df = df.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")

#wordCounts = words.groupBy("word").count()

query = df.writeStream.format("console").start()

query.awaitTermination()