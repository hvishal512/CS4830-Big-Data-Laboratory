from __future__ import print_function 
from pyspark import SparkContext
import pyspark.sql.types as tp
from pyspark.sql import Row
from pyspark.streaming import StreamingContext
from pyspark.ml.classification import LogisticRegression
from pyspark.streaming.kafka import KafkaUtils
import os
from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, FloatType
from pyspark.ml.feature import VectorAssembler,StringIndexer, VectorIndexer, StandardScaler,IndexToString 
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.session import SparkSession
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import json
from pyspark.sql import SQLContext

kafka_topic = 'from-pubsub'
zk = '10.182.0.2:2181' # Apache ZooKeeper quorum
app_name = 'from-pubsub' # Can be some other name
sc = SparkContext(appName="KafkaPubsub")
ssc = StreamingContext(sc, 20)
model = PipelineModel.load("gs://bdl__project20/model/")
schema = StructType([StructField("sl", FloatType(), True), StructField("sw", FloatType(), True),StructField("pl", FloatType(), True),StructField("pw", FloatType(), True)])
#spark = SparkSession.builder.appName(app_name).config("spark.master", "local").getOrCreate()
spark = SparkSession(sc)
kafkaStream = KafkaUtils.createStream(ssc, zk, app_name, {kafka_topic: 1})
#kafkaStream.pprint()
print("Starting")
def get_prediction(temp):
	print("Inside IF")
	temp.toDF().show()
	dstream = temp.map(lambda x: json.loads(x[1]))
	dstream.toDF().show()
	dat = dstream.map(lambda x:x[0])
	dat.toDF().show()
	#dat1 = dat.map(lambda x: x.split(','))
	#dat2 = dat1.map(lambda x: [float(i) for i in x])
	#dat3 = dat2.map(lambda x: x[1:])
	dat4 = dat.map(lambda x: Row(text=str(x)))
	#df = dat.toDF().toPandas()
	#df.columns = ['text']
	#print(df)
	df1 = spark.createDataFrame(dat4)
	df1.show()
	pred = model.transform(df1)
	pred.show()
kafkaStream.foreachRDD(lambda x: get_prediction(x))
print("Ending")
ssc.start()
ssc.awaitTermination()