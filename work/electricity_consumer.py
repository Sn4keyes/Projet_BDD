#!/usr/bin/python

from logging import root
from pandas.tseries import offsets
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import pymongo
import json
import time

BROKER = 'kafka:9093'
TOPIC = 'electricity0'

# def read_mongo(db, collection, query={}, host='localhost', port=27017, username=None, password=None, no_id=True):
#     db = _connect_mongo(host=host, port=port, username=root, password=password, db=db)
#     cursor = db[collection].find(query)
#     df =  pd.DataFrame(list(cursor))
#     if no_id:
#         del df['_id']
#     return df

def post_in_db(df, electricity_coll):
    post_db = electricity_coll.insert_many(df.to_dict('records'))

def connect_db():
    client = MongoClient('mongo', port = 27017, username = 'root', password = 'root')
    electricity_db = client.electricity
    electricity_coll = electricity_db.electricity_coll
    return electricity_db, electricity_coll

if __name__ == "__main__":
    try:
        print("########## ########## ########## ########## ########## ##########")
        print("- Creating BDD...")
        electricity_db, electricity_coll = connect_db()
        print("- OK")
    except:
        print("- MongoDB database connection error")
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=[BROKER], api_version=(2,6,0))
    for msg in consumer:
        print("- Awaiting data...")
        df = pd.DataFrame.from_dict(json.loads(msg.value))
        print("- OK")
        print("- DataFrame Pandas :\n")
        print(df)
        post_in_db(df, electricity_coll)
        print("########## ########## ########## ########## ########## ##########")