#!/usr/bin/python

from kafka import KafkaProducer
import preprocessor as p
from tweepy import *
import pandas as pd
import numpy as np
import tweepy
import json
import sys

BROKER = 'localhost:9092'
TOPIC = 'twitter0'
CONSUMER_KEY = "yKJujkZBJz9kToLpb6HTOuLc8"
CONSUMER_SECRET = "T9qpssjfGIQ5hHyPBEHSOLICaIbDXfuXkIR46T3T2Fa42dLhkI"
ACCESS_KEY = "1471777355517632512-XR1CPbOqQhXltAaUSNBQMOFSeWan6R"
ACCESS_SECRET = "QHAhCCNrmOXAu9i3QFcjp6AuOWzqh8XQvOacznPiugSs5"

def call_twitter_api():
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
    api = tweepy.API(auth,wait_on_rate_limit=True)
    search_words = ["#bitcoin" "#BTC" "#Mining"] # Enter your key words
    json_create = {"created_at" : [],
                    "text" : [],
                    "user_screen_name" : [],
                    "user_location" : []}
    for tweet in tweepy.Cursor(api.search_tweets,q=search_words,count=5,
                            lang="en",
                            since_id=0).items():
        json_create["created_at"].insert(-1, tweet.created_at.strftime("%m/%d/%Y"))
        json_create["text"].insert(-1, tweet.text)
        json_create["user_screen_name"].insert(-1, tweet.user.screen_name)
        json_create["user_location"].insert(-1, tweet.user.location)
    return json_create

if __name__ == "__main__":
    try:
        producer = KafkaProducer(bootstrap_servers=BROKER)                                                                         
    except Exception as e:
        print(f"ERROR --> {e}")
        sys.exit(1)
    
    # while True:
    print("########## ########## ########## ########## ########## ##########")
    print("- Send Data To Kafka consumer...")
    json_full = call_twitter_api()
    print(json_full)
    producer.send(TOPIC, json.dumps(json_full).encode('utf-8'))
    producer.flush()
    print("- OK")
    # sleep(5)
    print("########## ########## ########## ########## ########## ##########")
