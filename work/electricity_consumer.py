#!/usr/bin/python

from logging import root
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType
from pyspark.ml.stat import Correlation
from pyspark.sql import SparkSession
from pandas.tseries import offsets
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import pymongo
import json
import time

BROKER = 'kafka:9093'
TOPIC = 'electricity0'

# def post_in_bdd(spark_df):
#     spark_df.write.format("mongo").mode("append").option("database","electricity").option("collection", "electricity_col").save()

# def spark_connect():
#     spark = SparkSession    \
#             .builder    \
#             .master('local')    \
#             .appName('Crypto')  \
#             .config("spark.mongodb.input.uri", "mongodb://root:root@mongo:27017/crypto.*?authSource=admin")  \
#             .config("spark.mongodb.output.uri", "mongodb://root:root@mongo:27017/crypto.*?authSource=admin") \
#             .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
#             .config("spark-jars-packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")  \
#             .getOrCreate()
#     spark.sparkContext
#     return spark

# mongo_client = MongoClient('localhost', 27017)
# db = mongo_client.electricity_data
# col = db.electricity_col
# username = 'root'
# password = 'root'


def post_in_bdd(electricity_col, df):
    data = df.to_dict()
    electricity_col.insert_one(data)

def _connect_mongo(host, port, username, password, db):

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)

    return conn[electricity_col]


def read_mongo(db, collection, query={}, host='localhost', port=27017, username=None, password=None, no_id=True):

    db = _connect_mongo(host=host, port=port, username=root, password=password, db=db)

    cursor = db[collection].find(query)

    df =  pd.DataFrame(list(cursor))

    if no_id:
        del df['_id']

    return df

if __name__ == "__main__":

    try:
        client = MongoClient('mongo', 27017, username = 'root', password = 'root')
        db_electricity = client.electricity
        electricity_col = db_electricity.electricity_col
        electricity_col.drop()
        print("########## Création de la base de données ##########")
    except:
        print("Erreur de connexion à MongoDB")
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=[BROKER], api_version=(2,6,0))

    for msg in consumer:
        print("########## Received Data From Producer : OK ##########")
        df = pd.DataFrame.from_dict(json.loads(msg.value))
        print("\n########## DataFrame Pandas : ##########\n")
        print(df)
        post_in_bdd(electricity_col, df)