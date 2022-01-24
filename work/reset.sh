#!/bin/sh

python electricity_consumer.py  reset &
python twitter_consumer.py reset &
python crypto_consumer.py reset &