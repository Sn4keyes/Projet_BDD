#!/bin/sh

python src/electricity_consumer.py  reset &
python src/twitter_consumer.py reset &
python src/crypto_consumer.py reset &