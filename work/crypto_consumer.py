#!/usr/bin/python

from pandas.tseries import offsets
from kafka import KafkaConsumer
from datetime import datetime
import pandas as pd
import sqlite3
import json
import time

BROKER = 'kafka:9093'
TOPIC = 'crypto5'
NAME_BDD = 'crypto.db'

def post_in_bdd(df, conn):
    print("\n- Post in BDD...")
    df.to_sql("bitcoin", conn, if_exists="append")
    pd.read_sql_query("select * from bitcoin;", conn)
    print("- OK")

def read_in_bdd(conn):
    print("- Read in BDD...")
    cur = conn.cursor()
    cur.execute("select * from bitcoin limit 50;")
    results = cur.fetchall()
    print(results)
    cur.close()
    print("- OK")

if __name__ == "__main__":

    try:
        print("########## ########## ########## ########## ########## ##########")
        print("- Creating BDD...")
        conn = sqlite3.connect(NAME_BDD)
        print("- OK")
    except:
        print("- SQL database connection error")
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=[BROKER], api_version=(2,6,0))
    for msg in consumer:
        print("- Awaiting data...")
        df = pd.DataFrame.from_dict(json.loads(msg.value))
        print("- OK")
        print("- DataFrame Pandas :\n")
        print(df)
        post_in_bdd(df, conn)
        read_in_bdd(conn)
        print("########## ########## ########## ########## ########## ##########")
    conn.close()