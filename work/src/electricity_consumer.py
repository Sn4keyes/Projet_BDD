#!/usr/bin/env python

from logging import root
from os import stat
from pandas.tseries import offsets
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import json
import time
import sys

BROKER = 'kafka:9093'
TOPIC = 'electricity0'

# def read_mongo(db, collection, query={}, host='localhost', port=27017, username=None, password=None, no_id=True):
#     db = _connect_mongo(host=host, port=port, username=root, password=password, db=db)
#     cursor = db[collection].find(query)
#     df =  pd.DataFrame(list(cursor))
#     if no_id:
#         del df['_id']
#     return df

def post_in_electricity_db(df, electricity_coll):
    post_db = electricity_coll.insert_many(df.to_dict('records'))

def reset_electricity_db():
    client = MongoClient('mongo', port = 27017, username = 'root', password = 'root')
    electricity_db = client.electricity
    electricity_coll = electricity_db.electricity_coll
    electricity_coll.drop()
    return electricity_db, electricity_coll

def connect_electricity_db():
    client = MongoClient('mongo', port = 27017, username = 'root', password = 'root')
    electricity_db = client.electricity
    electricity_coll = electricity_db.electricity_coll
    return electricity_db, electricity_coll

def check_condition(state):
    if state == "start":
        print("- Start :")
        electricity_db, electricity_coll = connect_electricity_db()
        print("- Ok")
    elif state == "restart":
        print("- Restart :")
        electricity_db, electricity_coll = connect_electricity_db()
        print("- Ok")
    else:
        print("- Reset :")
        electricity_db, electricity_coll = reset_electricity_db()
        print("- Ok")
    return electricity_db, electricity_coll

if __name__ == "__main__":
    state = sys.argv[1]
    try:
        print("########## ########## ########## ########## ########## ##########")
        print("- Creating Electricity DB...")
        electricity_db, electricity_coll = check_condition(state)
        print("- OK")
    except:
        print("- MongoDB Electricity database connection error")
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=[BROKER], api_version=(2,6,0))
    for msg in consumer:
        print("- Awaiting Electricity data...")
        df = pd.DataFrame.from_dict(json.loads(msg.value))
        print("- OK")
        print("- DataFrame Pandas :\n")
        print(df)
        post_in_electricity_db(df, electricity_coll)
        print("########## ########## ########## ########## ########## ##########")