### Written by H.Vishal MM16B023 at 11:17 AM on 07/03/2020 ###

from google.cloud import pubsub_v1
from google.cloud import storage

subscriber = pubsub_v1.SubscriberClient()
topic_name = 'projects/hopeful-buckeye-266720/topics/topic_lab6'
subscription_name = 'projects/hopeful-buckeye-266720/subscriptions/subscript'
subscriber = pubsub_v1.SubscriberClient()
subscriber.create_subscription(name=subscription_name, topic=topic_name)

def callback(package):
    x = package
    print(x.data)
    print('recieved')
    with open('addresses.csv', 'r') as f: 
        count = 0
        for line in f:
            count = count+1
    print('Expected line count: ' + str(count)) 
    package.ack()

future = subscriber.subscribe(subscription_name, callback)

try:
    future.result()
except KeyboardInterrupt:
    future.cancel()
