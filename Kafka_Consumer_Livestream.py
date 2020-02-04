from kafka import KafkaConsumer
import json 
from pymongo import MongoClient
import pymongo

consumer = KafkaConsumer('Pizza_1', bootstrap_servers='localhost:9092', auto_offset_reset='latest', 
                        enable_auto_commit='True', consumer_timeout_ms=100000)

client=MongoClient('localhost',27017)
collection = client.twitter.Pizza     #<-- creates the collection and DB, then check it in MongoDB,if the data is loaded.

#Step to insert the json file into mongodb
for message in consumer:
    #Decoding the message and loading the json file
    t=json.loads(message.value.decode('utf-8')) 
    collection.insert_one(t)