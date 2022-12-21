from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
import pyspark.sql.functions as f
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from itertools import chain
from pyspark.sql.window import Window

def eval_metrics(df, epoch_id):

    if df.count() > 0:
        eval_acc =  MulticlassClassificationEvaluator(labelCol="stars", predictionCol="prediction", metricName="accuracy")
        eval_f1 =  MulticlassClassificationEvaluator(labelCol="stars", predictionCol="prediction", metricName="f1")

        print("-"*50)
        print(f"Batch: {epoch_id}")
        print("-"*50)
        df.show(df.count())
        print(f"Accuracy: {eval_acc.evaluate(df):.4f}\nF1 score: {eval_f1.evaluate(df):.4f}")

    pass

spark = SparkSession.builder.appName("kaushik").getOrCreate()
spark.sparkContext.setLogLevel("FATAL")

BROKER_IP = "10.138.0.2:9092"
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BROKER_IP).option("subscribe","project-topic").load()


df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
split_cols = f.split(df.value,'%')

df = df.withColumn('stars',split_cols.getItem(0))
df = df.withColumn('text',split_cols.getItem(1))

df.na.drop('any')
df = df.withColumn('stars',df['stars'].cast('double'))

df.createOrReplaceTempView('kaushik')

model = PipelineModel.load('gs://murari/modelpath/')


output_df = model.transform(df)

output_df = output_df.withColumn('correct' , f.when(f.col('prediction')==f.col('stars'),1).otherwise(0))
df_acc = output_df.select(f.format_number(f.avg('correct')*100,2).alias('accuracy'))

output_df2 = output_df[['prediction','stars']]
output_df2.createOrReplaceTempView('output')

query = output_df2.writeStream.foreachBatch(eval_metrics).start()
query.awaitTermination()
