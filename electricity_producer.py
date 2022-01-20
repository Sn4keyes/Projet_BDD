#!/usr/bin/python

from pycoingecko import CoinGeckoAPI
from kafka import KafkaProducer
import numpy as np
import requests
import json
import time
import sys

BROKER = 'localhost:9092'
TOPIC = 'electricity0'

list_code = ["DE","FR","CA-ON"]

def call_electricity_api():
    myToken = 'JBolGXLpHpqhxpjeRbVKUt7onZB8bpMS'
    head = {'auth-token': myToken}
    df = []
    for i in list_code:
        time.sleep(2)
        myUrl = 'https://api.co2signal.com/v1/latest?countryCode=%s' % (i)
        response = requests.get(myUrl, headers=head)
        electricity_data = response.json()
        df.append(electricity_data)
    return df


if __name__ == "__main__":
    
    try:
        producer = KafkaProducer(bootstrap_servers=BROKER)                                                                         
    except Exception as e:
        print(f"ERROR --> {e}")
        sys.exit(1)
    cg = CoinGeckoAPI()
    
    # while True:
    print("########## Send Data To Kafka: OK ##########")
    electricity_json = call_electricity_api()
    producer.send(TOPIC, json.dumps(electricity_json).encode('utf-8'))
    producer.flush()
    # sleep(5)