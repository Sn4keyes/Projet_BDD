#!/bin/sh

python electricity_consumer.py  restart &
python twitter_consumer.py restart &
python crypto_consumer.py restart &