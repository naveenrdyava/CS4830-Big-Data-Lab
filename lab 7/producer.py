from kafka import KafkaProducer
import csv
from google.cloud import storage

client = storage.Client()
bucket = client.get_bucket("lab-7-bucket")
blob = bucket.get_blob("iris.csv")
x = blob.download_as_string()

producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')
messages = x.split("\r\n")
del(messages[0])
for message in messages:
    print(message)
    producer.send("lab7_topic", message)
    producer.flush()
