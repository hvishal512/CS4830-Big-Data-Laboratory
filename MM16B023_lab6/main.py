def message(data, context):
    from google.cloud import pubsub_v1
    publish_client = pubsub_v1.PublisherClient()
    topic_name = 'projects/hopeful-buckeye-266720/topics/topic_lab6'
    publisher = pubsub_v1.PublisherClient()
    publisher.create_topic(topic_name)
    output = data['name']
    output = output.encode("utf-8")
    publish_client.publish(topic_name, output)
    print("The massaged has been recieved")
   
