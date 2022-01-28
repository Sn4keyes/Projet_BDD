#!/bin/sh

python src/electricity_producer.py &
python src/twitter_producer.py &
python src/crypto_producer.py &