import storm
from datetime import date
import time
import datetime
import boto.dynamodb
import json
import re
import redis

today = date.today()

#Connect to redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

TERMS={}

#-------- Load Sentiments Dict ----
sent_file = open('AFINN-111.txt')
sent_lines = sent_file.readlines()
for line in sent_lines:
    s = line.split("\t")
    TERMS[s[0]] = s[1]

sent_file.close()

#-------- Find Sentiment  ----------
def findsentiment(tweet):
    sentiment=0.0

    if tweet.has_key('text'):
        text = tweet['text']
        text=re.sub('[!@#$)(*<>=+/:;&^%#|\{},.?~`]', '', text)
        splitTweet=text.split()

        for word in splitTweet:
            if TERMS.has_key(word):
                sentiment = sentiment+ float(TERMS[word])

    return text.encode('utf-8'), sentiment, tweet['location']

def analyzeData(data):
    #Add your code for data analysis here
    tweet = json.loads(data)
    text, sentiment, location = findsentiment(tweet)
    return text, sentiment, location
    #return True

class MyBolt(storm.BasicBolt):
    def process(self, tup):
        data = tup.values[0]
        text, sentiment, location = analyzeData(data)
        result= "Result: "+ str(sentiment)
        #Store analyzed results in redis
        key = str(text)
        item_data = {
            "sentiment": sentiment,
            "location": location
        }
        r.set(key, json.dumps(item_data))
        storm.emit([result])


MyBolt().run()
