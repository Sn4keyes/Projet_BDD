#!/usr/bin/python

from os import stat
from pandas.tseries import offsets
from kafka import KafkaConsumer
import pymongo
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import json
import time
import sys

BROKER = 'kafka:9093'
TOPIC = 'twitter0'

def post_in_twitter_db(df, twitter_coll):
    post_db = twitter_coll.insert_many(df.to_dict('records'))

def restart_twitter_db():
    client = MongoClient('mongo', 27017, username = 'root', password = 'root')
    twitter_db = client.twitter
    twitter_coll = twitter_db.twitter_coll
    twitter_coll.drop()
    return twitter_db, twitter_coll

def connect_twitter_db():
    client = MongoClient('mongo', 27017, username = 'root', password = 'root')
    twitter_db = client.twitter
    twitter_coll = twitter_db.twitter_coll
    return twitter_db, twitter_coll

def check_condition(state):
    if state == "start":
        print("- Start :")
        twitter_db, twitter_coll = connect_twitter_db()
        print("- Ok")
    elif state == "restart":
        print("- Restart :")
        twitter_db, twitter_coll = connect_twitter_db()
        print("- Ok")
    else:
        print("- Reset :")
        twitter_db, twitter_coll = restart_twitter_db()
        print("- Ok")
    return twitter_db, twitter_coll

if __name__ == "__main__":
    state = sys.argv[1]
    try:
        print("########## ########## ########## ########## ########## ##########")
        print("- Creating Twitter DB...")
        twitter_db, twitter_coll = check_condition(state)
        print("- OK")
    except:
        print("- MongoDB Twitter database connection error")
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=[BROKER], api_version=(2,6,0))
    for msg in consumer:
        print("- Awaiting Twitter data...")
        df = pd.DataFrame.from_dict(json.loads(msg.value))
        print("- OK")
        print("- DataFrame Pandas :\n")
        print(df)
        post_in_twitter_db(df, twitter_coll)
        print("########## ########## ########## ########## ########## ##########")