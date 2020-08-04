from google.cloud import pubsub_v1

proj_id = 'big-query-horse-play'

topic_id = 'data-source-update-status'

publisher_client = pubsub_v1.PublisherClient()
topic_path = publisher_client.topic_path(proj_id, topic_id)
message_data = 'test from python api'.encode('utf-8')
message = publisher_client.publish(topic_path, message_data, key1='test', key2='test')

print(message.result())
