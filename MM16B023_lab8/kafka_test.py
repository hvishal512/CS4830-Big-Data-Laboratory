from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexerModel, IndexToString, StandardScaler, MinMaxScaler, PolynomialExpansion
from pyspark.ml import PipelineModel
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import Row

kafka_topic = 'from-pubsub'

zk = '10.138.0.2:2181' # Apache ZooKeeper quorum

app_name = 'from-pubsub' # Can be some other name
sc = SparkContext(appName= "KafkaPubsub" )
ssc = StreamingContext(sc, 50 )
spark = SparkSession(sc)

kafkaStream = KafkaUtils.createStream(ssc, zk, app_name, {kafka_topic: 1})

#parse the kafka stream

def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def process(rdd):

    spark = getSparkSessionInstance(rdd.context.getConf())

    dota = rdd.map(lambda x: x[1])
    featuresdata = dota.map(lambda x: x.split(':')[2])
    actualdata = featuresdata.map(lambda x: x.split(','))
    rowRdd = actualdata.map(lambda x: Row(sl=float(x[0][1:]), sw=float(x[1]), pl=float(x[2]), pw=float(x[3]), stringlabel=x[4][:-4]))
    features = spark.createDataFrame(rowRdd)
    features.show()
    rowRdd = actualdata.map(lambda x: Row(sl=float(x[0]), sw=float(x[1]), pl=float(x[2]), pw=float(x[3]), stringlabel=x[4]))
    
    indexer = StringIndexerModel()
    assembler = VectorAssembler()
    lr = LogisticRegressionModel()

    pipe = PipelineModel(stages=[indexer,assembler,lr]).load('gs://suryasuresh/lab8output')

    result = pipe.transform(features)

    f1score = MulticlassClassificationEvaluator(metricName='f1')
    precision = MulticlassClassificationEvaluator(metricName='weightedPrecision')
    recall = MulticlassClassificationEvaluator(metricName='weightedRecall')
    accuracy = MulticlassClassificationEvaluator(metricName='accuracy')

    print(result.values)
    print("Accuracy:\t",accuracy.evaluate(result),"\nF1score:\t",f1score.evaluate(result),"\nWeighted Recall:\t",recall.evaluate(result),"\nWeighted Precision:\t",precision.evaluate(result))

kafkaStream.foreachRDD(process)

#predict output 

# indexer = StringIndexerModel()
# assembler = VectorAssembler()
# lr = LogisticRegressionModel()

# pipe = PipelineModel(stages=[indexer,assembler,lr]).load('gs://suryasuresh/lab8output')

# result = pipe.transform(finalDataFrame)

# f1score = MulticlassClassificationEvaluator(metricName='f1')
# precision = MulticlassClassificationEvaluator(metricName='weightedPrecision')
# recall = MulticlassClassificationEvaluator(metricName='weightedRecall')
# accuracy = MulticlassClassificationEvaluator(metricName='accuracy')

# print("Accuracy:\t",accuracy.evaluate(result),"\nF1score:\t",f1score.evaluate(result),"\nWeighted Recall:\t",recall.evaluate(result),"\nWeighted Precision:\t",precision.evaluate(result))

ssc.start() # Start the computation
ssc.awaitTermination() # Wait for the computation to terminate
