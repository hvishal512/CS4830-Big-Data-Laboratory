from __future__ import print_function  
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import os
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import pyspark.sql.types as tp
from pyspark.ml.feature import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator, VectorAssembler
from pyspark.ml.feature import StopWordsRemover, Word2Vec, RegexTokenizer
from pyspark.ml.classification import LogisticRegression
#from pyspark.mllib.feature import HashingTF, IDF

sc = SparkContext(appName="Project")
spark = SparkSession(sc)

spark_df = spark.read.format("bigquery").option("table", "bdl_project20.yelp").load().toDF("text", "cool", "funny", "stars", "date", "business_id","user_id","useful","review_id")

spark_df.show(10)

tok =  RegexTokenizer(inputCol= "text" , outputCol= "words", pattern= '\\W')
stopword_rm = StopWordsRemover(inputCol="words", outputCol="words_nsw")
hashingTF = HashingTF(inputCol="words_nsw", outputCol="rawFeatures")
idf = IDF(inputCol="rawFeatures", outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="stars")
pipe = Pipeline(stages=[tok, stopword_rm,hashingTF,idf,lr])
model = pipe.fit(spark_df)
model.save("gs://bdl__project20/model/")
predictions = model.transform(spark_df)

predictions.show(10)

evaluator = MulticlassClassificationEvaluator(labelCol="stars", predictionCol="prediction", metricName="f1")

f1 = evaluator.evaluate(predictions)

print("F1 score =", f1)