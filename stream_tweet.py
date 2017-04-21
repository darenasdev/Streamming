import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from kafka import KafkaProducer
#from pylastic import pylastic
from ProdKaf import ProdKaf
import time
import string
import config
import json




class MyListener(StreamListener):
    """Custom StreamListener for streaming data."""

    def __init__(self):
        StreamListener.__init__(self)
        self.producer = ProdKaf('localhost:9092')

    def on_data(self,data):
        try:
            data_dict = json.loads(data)
            data_dict["text"].encode('utf-8').decode('utf8')
            id_tweet = data_dict["id"]
            self.producer.sendTweet('test-tweet', id_tweet, data_dict)
           
            #return True
        
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True

if __name__ == '__main__':
   
    listener = MyListener()

    auth = OAuthHandler(config.consumer_key, config.consumer_secret)
    auth.set_access_token(config.access_token, config.access_token_secret)
    #api = tweepy.API(auth)

    twitter_stream = Stream(auth, listener)
    #twitter_stream.filter(track=['#PRI'])
    twitter_stream.filter(track=['PRI','PAN','MORENA'],languages=["es"])
    