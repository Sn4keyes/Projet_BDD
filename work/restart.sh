#!/bin/sh

python src/electricity_consumer.py  restart &
python src/twitter_consumer.py restart &
python src/crypto_consumer.py restart &