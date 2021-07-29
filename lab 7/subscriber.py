from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
import pyspark.sql.functions as f
from itertools import chain
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("iris_predictor").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "10.182.0.4:9092").option("subscribe","lab7_topic").load()

# code snippet to split the strings at commas to seperate coulmns in csv.
split_cols = f.split(df.value,',')

# the below code assigns names to the features(headers to the columns of the data frame)
df = df.withColumn('sepal_length',split_cols.getItem(0))
df = df.withColumn('sepal_width',split_cols.getItem(1))
df = df.withColumn('petal_length',split_cols.getItem(2))
df = df.withColumn('petal_width',split_cols.getItem(3))
df = df.withColumn('class',split_cols.getItem(4))

df.na.drop('any')

# converting the string format to float format to be usable for model predictions.
for col in ['sepal_length','sepal_width','petal_length','petal_width']:
    df = df.withColumn(col,df[col].cast('float'))

df.createOrReplaceTempView('iris')

# code to load the saved iris model from lab 5.
model = PipelineModel.load('gs://lab-7-bucket/RFC_lab_5_iris_model/')

assembler = VectorAssembler(inputCols=['sepal_length','sepal_width','petal_length','petal_width'],
                            outputCol = "true_features")
df = assembler.transform(df)
df = df.withColumn('true_label',df['class'])

mapping = dict(zip(['Iris-setosa','Iris-versicolor','Iris-virginica'],[0.0,1.0,2.0]))
mapping_expr = f.create_map([f.lit(x) for x in chain(*mapping.items())])
df = df.withColumn('true_label',mapping_expr[f.col("class")])

predictions = model.transform(df)

mapping = dict(zip([0.0,1.0,2.0],['setosa','versicolor','virginica']))
mapping_expr = f.create_map([f.lit(x) for x in chain(*mapping.items())])
output_df = predictions.withColumn('prediction_class',mapping_expr[f.col("prediction")])[['prediction_class','true_label','prediction']]


output_df = output_df.withColumn('correct' , f.when(f.col('prediction')==f.col('true_label'),1).otherwise(0))

df_acc = output_df.select(f.format_number(f.avg('correct')*100,2).alias('accuracy'))

output_df2 = output_df[['prediction_class','prediction','true_label','correct']]
output_df2.createOrReplaceTempView('output')
query1 = output_df2.writeStream.queryName("output").outputMode('append').format('console').start()
query2 =  df_acc.writeStream.outputMode("complete").format("console").start()
query1.awaitTermination()
query2.awaitTermination()