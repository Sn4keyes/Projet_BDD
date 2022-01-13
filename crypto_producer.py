#!/usr/bin/python

from pycoingecko import CoinGeckoAPI
from kafka import KafkaProducer
import numpy as np
import json
import time
import sys

BROKER = 'localhost:9092'
TOPIC = 'crypto5'
list_crypto = [
    'bitcoin',
]

def clean_json(json, crypto_name):
    json_clean = {
        crypto_name + "_timestamp": [],
        crypto_name + "_prices": [],
        crypto_name + "_market_cap": [],
        crypto_name + "_total_vol": []
    }
    js_line = 0
    for js_col in json:
        middle_js_line = len(json[js_col][js_line]) // 2
        for i in json[js_col]:
            if js_line <= 168:
                if json[js_col] == json["prices"]:
                    json_clean[crypto_name + "_timestamp"].extend(json[js_col][js_line][:middle_js_line])
                    json_clean[crypto_name + "_prices"].extend(json[js_col][js_line][middle_js_line:])
                elif json[js_col] == json["market_caps"]:
                    json_clean[crypto_name + "_market_cap"].extend(json[js_col][js_line][middle_js_line:])
                elif json[js_col] == json["total_volumes"]:
                    json_clean[crypto_name + "_total_vol"].extend(json[js_col][js_line][middle_js_line:])
                js_line += 1
        js_line = 0
    return json_clean

def producer_bitcoin():
    bitcoin = cg.get_coin_market_chart_by_id(
                id="bitcoin",
                tickers=False,
                vs_currency='usd',
                include_market_cap=True,
                days='7'
            )
    clean_bitcoin = clean_json(bitcoin, "BTC")
    return clean_bitcoin

def call_crypto_api():
    json_full = {}
    
    bitcoin = producer_bitcoin()
    json_full.update(bitcoin)

    return json_full

if __name__ == "__main__":
    
    try:
        producer = KafkaProducer(bootstrap_servers=BROKER)                                                                         
    except Exception as e:
        print(f"ERROR --> {e}")
        sys.exit(1)
    cg = CoinGeckoAPI()
    
    # while True:
    print("########## Send Data To Kafka: OK ##########")
    json_full = call_crypto_api()
    producer.send(TOPIC, json.dumps(json_full).encode('utf-8'))
    producer.flush()
    # sleep(5)