import tweepy
import threading, logging, time
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer
from kafka import KafkaConsumer,KafkaProducer 
from kafka import KafkaConsumer,KafkaProducer 
import json
import string

consumer_key = 'g1Xt8LsKPTXuGLwxEQlxLfo39'
consumer_secret = 'V9crbbx1CgOyWnyrne06DFfeSwXLMBzG17SY7GOHq4fhdNp6Yd'
access_token = '1220847667166810112-fDjAj2PDK5tV2I0btlvXVVjO1QGKx3'
access_token_secret = 'ztadKDxjoKEg6rbY5YQArHv7pzNQtpO0JMsHHyjEdVw0U'

class StdOutListener(tweepy.StreamListener):  #Override the class
    def on_data(self, data):
        #Encoding the data before sending it
        producer.send(my_topic, data.encode('utf-8')) 
        return True

    def on_error(self, status_code):
        print('Got an error with status code: ' + str(status_code))
        return True # To continue listening
 
    def on_timeout(self):
        print('Timeout...')
        return True # To continue listening

if __name__ == '__main__':
    
    listener = StdOutListener()

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)

    stream = tweepy.Stream(auth, listener)
    #adding a kafka Topic if not present it will send manually
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    my_topic = 'Pizza_1'
    #In this section topic to be streamed is added and language is also selected
    stream.filter(languages=["en"], track=['pizza', '@pizza'], is_async=True)