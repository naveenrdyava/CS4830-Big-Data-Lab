#Import Required Packages
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, MinMaxScaler 
from pyspark.ml.classification import LogisticRegression, OneVsRest, RandomForestClassifier, DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# Initialising spark session
sc = SparkContext()
spark = SparkSession(sc)

#Reading data from table
data = spark.read.format("bigquery").option(
    "table", "iris_dataset.iris_input").load()

# Avoid rows with NULL Value from the table
# Create a view so that Spark SQL queries can be run against the data
data.createOrReplaceTempView("iris_data")
data.show()

#convert strings in class column to feature encoded values
indexer = StringIndexer(inputCol='class', outputCol='label').fit(data)

#test and train split
train_data,test_data=data.randomSplit([0.8,0.2])

# Vector assembler converts the input features to required format
assembler=VectorAssembler(inputCols=['sepal_length',
									 'sepal_width',
									 'petal_length',
									 'petal_width'],				
								outputCol='features')

# Scales the input features.
scaler=MinMaxScaler(inputCol='features', outputCol="features_scaled")

# Random forest Classifier.
RFC = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=13)

# logistic regression Classifier
LRC = LogisticRegression(maxIter=40, regParam=0.2, elasticNetParam=0.7)

# One Vs Rest Classifier.
OVRC = OneVsRest(classifier=LogisticRegression(maxIter=50, tol=1E-6, fitIntercept=True))

# Decision Tree Classifier.
DTC = DecisionTreeClassifier(maxDepth=5, labelCol="label")


def printer(model, Banner=''):	
	train_prediction = model.transform(train_data).select("prediction", "label")
	test_prediction = model.transform(test_data).select("prediction", "label")
	print(Banner) # prints the model name.
	# prints the accuracy on training data
	print("Training accuracy = " ,MulticlassClassificationEvaluator(metricName="accuracy").evaluate(train_prediction))
	# prints the accuracy on test data
	print("Test accuracy = " ,MulticlassClassificationEvaluator(metricName="accuracy").evaluate(test_prediction))

pipe= Pipeline(stages=[indexer, assembler, RFC])
model = pipe.fit(train_data)
printer(model,'Random Forest Model')

pipe= Pipeline(stages=[indexer, assembler, scaler, LRC])
model = pipe.fit(train_data)
printer(model,'Logistic regression Model with scaler')

pipe= Pipeline(stages=[indexer, assembler, scaler, DTC])
model = pipe.fit(train_data)
printer(model,'Decision Tree Classifier Model with scaler')

pipe= Pipeline(stages=[indexer, assembler, scaler, OVRC])
model = pipe.fit(train_data)
printer(model,'One-versus-Rest classifier Model with scaler')