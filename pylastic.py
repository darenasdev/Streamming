from elasticsearch import Elasticsearch

class pylastic:

    def __init__(self,url):
        self.url = url
        self.es = Elasticsearch([url])

    def sendTweet(self, index, doc_type, id, doc):
        res = self.es.index(index=index, doc_type=doc_type, id=id, body=doc)

"""

es = Elasticsearch(['http://search-beevagrad-yzavdnk3vgybj33teqgucq7ray.us-east-1.es.amazonaws.com'])

doc = {
    'tweet': 'kimchy',
}
res = es.index(index="daniel", doc_type='tweet', body=doc)
print(res['created'])

res = es.get(index="daniel", doc_type='tweet')
print(res['_source'])

es.indices.refresh(index="daniel")

res = es.search(index="daniel", body={"query": {"match_all": {}}})
print("Got %d Hits:" % res['hits']['total'])
for hit in res['hits']['hits']:
    print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])

    """