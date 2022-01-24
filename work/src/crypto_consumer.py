#!/usr/bin/python

import os
from pandas.tseries import offsets
from kafka import KafkaConsumer
from datetime import datetime
import pandas as pd
import sqlite3
import json
import time
import sys

BROKER = 'kafka:9093'
TOPIC = 'crypto6'
NAME_BDD = "crypto.db"

def post_in_bdd(df, conn):
    print("\n- Post in BDD...")
    df.to_sql("bitcoin", conn, if_exists="replace")
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

def reset_crypto_db():
    conn = sqlite3.connect(NAME_BDD)
    c = conn.cursor()
    c.execute("DROP TABLE bitcoin")
    c.execute('''
            CREATE TABLE IF NOT EXISTS bitcoin
            ([index] INT PRIMARY KEY,
            [BTC_timestamp] FLOAT,
            [BTC_prices] FLOAT,
            [BTC_market_cap] FLOAT,
            [BTC_total_vol] FLOAT)
            ''')
    conn.commit()
    return conn  

def connect_crypto_db():
    conn = sqlite3.connect(NAME_BDD)
    c = conn.cursor()
    c.execute('''
            CREATE TABLE IF NOT EXISTS bitcoin
            ([index] INT PRIMARY KEY,
            [BTC_timestamp] FLOAT,
            [BTC_prices] FLOAT,
            [BTC_market_cap] FLOAT,
            [BTC_total_vol] FLOAT)
            ''')
    conn.commit()
    return conn

def check_condition(state):
    if state == "start":
        print("- Start :")
        conn = connect_crypto_db()
        print("- Ok")
    elif state == "restart":
        print("- Restart :")
        conn = connect_crypto_db()
        print("- Ok")
    else:
        print("- Reset :")
        conn = reset_crypto_db()
        print("- Ok")
    return conn

if __name__ == "__main__":
    state = sys.argv[1]
    try:
        print("########## ########## ########## ########## ########## ##########")
        print("- Creating Crypto DB...")
        conn = check_condition(state)
        print("- OK")
    except:
        print("- SQL Crypto database connection error")
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=[BROKER], api_version=(2,6,0))
    for msg in consumer:
        print("- Awaiting Crypto data...")
        df = pd.DataFrame.from_dict(json.loads(msg.value))
        print("- OK")
        print("- DataFrame Pandas :\n")
        print(df)
        post_in_bdd(df, conn)
        read_in_bdd(conn)
        print("########## ########## ########## ########## ########## ##########")
    conn.close()