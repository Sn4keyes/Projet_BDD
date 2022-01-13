#!/usr/bin/python

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
TOPIC = 'twitter0'

def post_in_bdd(spark_df):
    spark_df.write.format("mongo").mode("append").option("database","twitter").option("collection", "twitter_col").save()

def spark_connect():
    spark = SparkSession    \
            .builder    \
            .master('local')    \
            .appName('Crypto')  \
            .config("spark.mongodb.input.uri", "mongodb://root:root@mongo:27017/crypto.*?authSource=admin")  \
            .config("spark.mongodb.output.uri", "mongodb://root:root@mongo:27017/crypto.*?authSource=admin") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
            .config("spark-jars-packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")  \
            .getOrCreate()
    spark.sparkContext
    return spark

if __name__ == "__main__":

    try:
        client = MongoClient('mongo', 27017, username = 'root', password = 'root')
        db_crypto = client.crypto
        crypto_col = db_crypto.crypto_col
        calcul_col = db_crypto.calcul_col
        crypto_col.drop()
        calcul_col.drop()
        print("########## Création de la base de données ##########")
    except:
        print("Erreur de connexion à MongoDB")
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=[BROKER], api_version=(2,6,0))
    spark = spark_connect()
    for msg in consumer:
        print("########## Received Data From Producer : OK ##########")
        df = pd.DataFrame.from_dict(json.loads(msg.value))
        print("\n########## DataFrame Pandas : ##########\n")
        print(df)
        print("\n########## Minimum : ##########")
        print(df["BNB_prices"].min())
        print("\n########## Maximum : ##########")
        print(df["BNB_prices"].max())
        print("\n########## Average : ##########")
        print(df["BNB_prices"].mean())
        spark_df = spark.createDataFrame(df)
        post_in_bdd(spark_df)
        print("\n########## DataFrame Spark : ##########\n")
        spark_df.show(5, False)