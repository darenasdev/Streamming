from kafka import KafkaConsumer
from pylastic import pylastic
import json
"""
class consumerElas:

    def __init__(self,topic,url,url_es):
        self.topic = topic
        self.url = url
        self.consumer = KafkaConsumer(self.topic,bootstrap_servers=[self.url])
        self.url_es = url_es #'http://search-beevagrad-yzavdnk3vgybj33teqgucq7ray.us-east-1.es.amazonaws.com:80'
        self.es = pylastic(url_es)

    def savemessage(self):
        for msg in self.consumer:
            print(msg)
            tweet = json.loads(msg.value.decode('utf8'))
            text = tweet["text"]
            ide = str(int(msg.key))
            self.es.sendTweet('test-index','tweet',ide,{'text':text})            

            
"""
consumer = KafkaConsumer('test',bootstrap_servers=['localhost:9092'])
for msg in consumer:
    print (msg.key + ""+ msg.value)
"""

consumer = consumerElas('test','localhost:9092','localhost:9200')
consumer.savemessage()
"""