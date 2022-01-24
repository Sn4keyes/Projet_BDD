#!/bin/sh

python electricity_consumer.py  start &
python twitter_consumer.py start &
python crypto_consumer.py start &