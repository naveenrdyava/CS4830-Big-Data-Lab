import os
from google.cloud import pubsub_v1
from google.cloud import storage

client = storage.Client()
bucket = client.get_bucket('lab_6_pubsub')

subscriber = pubsub_v1.SubscriberClient()
topic_name = 'projects/{project_id}/topics/{topic}'.format(project_id='lab-6-308513', topic='lab6_topic')

subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(project_id='lab-6-308513', sub='lab6_subs')

def callback(message):
    blob = bucket.get_blob(message.data.decode('utf-8'))
    x = blob.download_as_string()
    x = x.decode('utf-8')
    print('\nThe number of lines in the file ',message.data.decode('utf-8'),' : ',len(x.split('\n')))
    message.ack()

future = subscriber.subscribe(subscription_name, callback)

try:
    future.result()
except KeyboardInterrupt:
    future.cancel()