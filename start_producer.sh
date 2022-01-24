#!/bin/sh

python electricity_producer.py &
python twitter_producer.py &
python crypto_pproducer.py &