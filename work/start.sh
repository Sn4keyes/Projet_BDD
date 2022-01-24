#!/bin/sh

python src/electricity_consumer.py  start &
python src/twitter_consumer.py start &
python src/crypto_consumer.py start &