from google.cloud import storage
from kafka import KafkaProducer
from time import sleep
import json
from json import dumps
from os import environ
from os import listdir
from os.path import isfile, join


bucketName = 'project-final'
bucketFolder = 'yelp_train.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucketName)
files = bucket.list_blobs(prefix=bucketFolder)

fileList = [file.name for file in files if file.name.endswith('.json')]
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda k: k.encode('utf-8'))

for filetemp in fileList : 
    blob = bucket.blob(filetemp)
    x = blob.download_as_string()
    x = x.decode('utf-8')
    data_str = x.split('\n')

    for each_data in data_str:
        message = json.loads(each_data)
        mess_send = str(message['stars']) + '%' + message['text']
        print(mess_send)
        producer.send("project-topic", value=mess_send)
        sleep(0.1)




