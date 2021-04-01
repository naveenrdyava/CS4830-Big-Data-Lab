
def lab6_cloudfncn(data, context):
        from google.cloud import pubsub_v1
        
        publisher = pubsub_v1.PublisherClient()
        topic_name = 'projects/{project_id}/topics/{topic}'.format(project_id='lab-6-308513', topic='lab6_topic')
        future = publisher.publish(topic_name, bytes(data['name'] ,'utf-8'))
        future.result()
