#!/usr/bin/python2.7

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import json
import time

import urllib2
import re
import sys
import copy

from constants import STATE_DIC, STATE_SET

from kafka.client import KafkaClient
from kafka.client import SimpleClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

#Connect to Kafka
client = SimpleClient("ip-172-31-18-27.ec2.internal:6667")
producer = SimpleProducer(client)

#Get Twitter API keys from Twitter developer account
access_token = "3108827146-XzlvkAaghCiRRYHa18jeksRZDiKAbCGBSqmrJhN"
access_token_secret = "wQ0canEo4rTzucfxynrBltiRBFI7sDHNwMN1dAr2rMTMl"
consumer_key = "Rf12Rmqt0fa2RtY4JjCOaO1Xx"
consumer_secret = "TK5mLA60LETRXQKRzHqHsc0eGiXnOFAdtTBADFWag9ZeUHpLhK"

#GOOGLE_API_KEY = "AIzaSyAv_M33Sg8TU7TpDtuzt2z42qbB7n8y6Hwi" #feng
GOOGLE_API_KEY = "AIzaSyDMmmIAYaakxzPiFFNdlEAYgy3JcSBWXYo"  #shiyu
#GOOGLE_API_KEY = "AIzaSyC4FHX0PfUH8Jtcm88kBet0gQ1N3VsbKJ4"  #ning

#Implement this function to publish data to Kafka topic
def publish(data):
    #datastr = json.dumps(data)
    producer.send_messages('sentiment', bytes(data))
    time.sleep(0.1)
    return True
#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def processLocation(self, loc):
        baseUrl = "https://maps.googleapis.com/maps/api/geocode/json?address="
        url = baseUrl + loc + "&key=" + GOOGLE_API_KEY
        #url = baseUrl + loc
        url = url.replace(" ", "%20")

        asciiurl = url.encode('ascii', 'ignore')
        raw = urllib2.urlopen(asciiurl)

        raw = raw.read()
        resp = json.loads(raw)
        #print resp
        state = None
        if len(resp["results"]) > 0:
            addr = str(resp["results"][0]["formatted_address"]).upper()
            if 'USA' in addr:
                state = re.sub("\d", "", addr.split(',')[-2]).strip()
                #print "state: " + state
                if state in STATE_DIC:
                    state = STATE_DIC[state]
                elif state not in STATE_SET:
                    state = None
                #print "shorten: " + state
        return state

    def on_data(self, data):
        tweet = json.loads(data)
        location = tweet['user']['location']
        if location:
            loc = self.processLocation(location)
            if not loc: return True
            print loc
            filtered_data = {}
            filtered_data['text'] = tweet['text']
            filtered_data['location'] = loc
            publish(json.dumps(filtered_data))
            #time.sleep(0.1)
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':
    print 'Listening...'
    #This handles Twitter authentification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    keyword = copy.deepcopy(sys.argv[1])

    #This line filter Twitter Streams to capture data by the keywords
    while True:
        try:
            stream.filter(track=[keyword], async=True)
        except:
            continue
