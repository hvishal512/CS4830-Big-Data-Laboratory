from __future__ import print_function 
from pyspark.context import SparkContext
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression , DecisionTreeClassifier
from pyspark.sql.session import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, StandardScaler, PolynomialExpansion, RFormula
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

sc = SparkContext()
spark = SparkSession(sc)

output_location = 'gs://suryasuresh/lab8output'

# Read the data from BigQuery as a Spark Dataframe
data = spark.read.format('csv').load('gs://suryasuresh/iris_test.csv').toDF("sl", "sw", "pl", "pw","stringlabel")
raw_data = data.withColumn('sl',data['sl'].cast("float")).withColumn('sw',data['sw'].cast("float")).withColumn('pl',data['pl'].cast("float")).withColumn('pw',data['pw'].cast("float"))
indexer = StringIndexer(inputCol='stringlabel',outputCol='label')
assembler = VectorAssembler(inputCols = ["sl", "sw", "pl", "pw"], outputCol = "features")
train, test = raw_data.randomSplit([0.8,0.2])

lr = LogisticRegression(maxIter = 1000, regParam = 0.3,elasticNetParam = 0.8)

#pipeline
pipe = Pipeline(stages=[indexer,assembler,lr])

#training the model
model = pipe.fit(train)
pred_test = model.transform(test)

result = model.transform(train)
model.write().overwrite().save(output_location)

#for corss validation
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

lr_evaluator = MulticlassClassificationEvaluator(predictionCol = "prediction", labelCol="label",metricName="accuracy")

paramGrid = ParamGridBuilder().addGrid(lr.regParam,[0.001,0.01,0.1,0.3,0.5]).build()

crossval = CrossValidator(estimator=pipe, estimatorParamMaps=paramGrid, evaluator=MulticlassClassificationEvaluator(),numFolds=3)
cvModel = crossval.fit(train)

prediction = cvModel.transform(test)
print(lr_evaluator.evaluate(prediction))
print("Coefficients:" + str(cvModel.bestModel.stages[-1].coefficientMatrix))
