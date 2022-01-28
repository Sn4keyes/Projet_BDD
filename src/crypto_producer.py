#!/usr/bin/python

from pycoingecko import CoinGeckoAPI
from kafka import KafkaProducer
import numpy as np
import json
import time
import sys

BROKER = 'localhost:9092'
TOPIC = 'crypto7'
crypto = "bitcoin"

def data_storage(crypto_json, crypto_clean, js_col, crypto_name, js_line, middle_js_line):
    if crypto_json[js_col] == crypto_json["prices"]:
        crypto_clean[crypto_name + "_timestamp"] \
            .extend(crypto_json[js_col][js_line][:middle_js_line])
        crypto_clean[crypto_name + "_prices"] \
            .extend(crypto_json[js_col][js_line][middle_js_line:])
    elif crypto_json[js_col] == crypto_json["market_caps"]:
        crypto_clean[crypto_name + "_market_cap"] \
            .extend(crypto_json[js_col][js_line][middle_js_line:])
    elif crypto_json[js_col] == crypto_json["total_volumes"]:
        crypto_clean[crypto_name + "_total_vol"] \
            .extend(crypto_json[js_col][js_line][middle_js_line:])

def clean_json(crypto_json, crypto_name):
    crypto_clean = {
        crypto_name + "_timestamp": [],
        crypto_name + "_prices": [],
        crypto_name + "_market_cap": [],
        crypto_name + "_total_vol": []}
    js_line = 0
    for js_col in crypto_json:
        middle_js_line = len(crypto_json[js_col][js_line]) // 2
        for i in crypto_json[js_col]:
            if js_line <= 168:
                data_storage(crypto_json, crypto_clean, js_col,
                crypto_name, js_line, middle_js_line)
                js_line += 1
        js_line = 0
    return crypto_clean

def producer_bitcoin():
    bitcoin = cg.get_coin_market_chart_by_id(
                id=crypto,
                tickers=False,
                vs_currency='usd',
                include_market_cap=True,
                days='1'
            )
    clean_bitcoin = clean_json(bitcoin, "BTC")
    return clean_bitcoin

def call_crypto_api():
    crypto_json = {}
    bitcoin = producer_bitcoin()
    crypto_json.update(bitcoin)
    return crypto_json

if __name__ == "__main__":
    try:
        print("########## ########## ########## ########## ########## ##########")
        producer = KafkaProducer(bootstrap_servers=[BROKER])                                                                         
    except Exception as e:
        print(f"ERROR --> {e}")
        sys.exit(1)
    cg = CoinGeckoAPI()
    
    # while True:
    print("- Send Data To Kafka consumer...")
    cryto_json = call_crypto_api()
    producer.send(TOPIC, json.dumps(cryto_json).encode('utf-8'))
    producer.flush()
    print("- OK")
    # sleep(5)
    print("########## ########## ########## ########## ########## ##########")