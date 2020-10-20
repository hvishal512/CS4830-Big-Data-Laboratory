from google.cloud import pubsub_v1
import time

publisher = pubsub_v1.PublisherClient()
tname = 'projects/refined-signer-266713/topics/to-kafka'
test_file = open('iris_test.csv', 'r')
lines = test_file.readlines()
for line in lines:
    publisher.publish(tname, ' ',dota =line)
    time.sleep(10)
