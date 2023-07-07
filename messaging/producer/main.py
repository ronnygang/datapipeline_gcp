from google.cloud import pubsub_v1
import pytz

def publish_message(event, context):
    file_name = event['name']
    topic_name = 'projects/ronny-dev-airflow/topics/queue'
    publisher = pubsub_v1.PublisherClient()
    publisher.publish(topic_name, data=file_name.encode('utf-8'))
    print('Mensaje publicado en el tema:', topic_name)