from kafka import KafkaConsumer
from pylastic import pylastic
from py_mongo import py_mongo
import collections, json

class consumerElas:

    def __init__(self,topic,url,url_es,url_mon):
        self.topic = topic
        self.url = url
        self.consumer = KafkaConsumer(self.topic,bootstrap_servers=[self.url])
        self.url_es = url_es 
        self.url_mon = url_mon
        #'http://search-beevagrad-yzavdnk3vgybj33teqgucq7ray.us-east-1.es.amazonaws.com:80'
        self.es = pylastic(url_es)
        self.mon = py_mongo(self.url_mon)

    def savemessage(self):
        for msg in self.consumer:
            #print("mensajeeee---->>"+str(msg))
            tweet = json.loads(str(msg.value.decode('UTF-8')))
            text = tweet["text"]
            print(text)
            ide = str(int(msg.key))
            print(ide)
            self.es.sendTweet('daniel','tweet',ide,{'text':text})
            self.mon.saveTweet('daniel','tweet',ide,tweet)            

            
"""
consumer = KafkaConsumer('test',bootstrap_servers=['localhost:9092'])
for msg in consumer:
    print (msg.key + ""+ msg.value)
    print(msg)
"""

consumer = consumerElas('test-tweet','localhost:9092','http://search-beevagrad-yzavdnk3vgybj33teqgucq7ray.us-east-1.es.amazonaws.com:80','54.174.5.92:27017')
consumer.savemessage()
