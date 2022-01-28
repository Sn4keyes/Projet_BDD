from datetime import datetime
from http import client
from re import X
from sqlite3 import Connection
from pathlib import Path
import streamlit as st
import altair as alt
import pandas as pd
import numpy as np
import sqlite3
import plotly.express as px
from pymongo import MongoClient

NAME_BDD = "crypto.db"

########## ELECTRICITY ##########
def display_electricity(client_connect):
    st.header("Electricity :")
    st.subheader("Simulation of the correlation between Bitcoin mining and the\
        carbon intensity of Bitcoin mining countries.")
    display_electricity_data(client_connect)

def display_electricity_data(client_connect: Connection):
    df_electricity = get_electricity_data(client_connect)
    st.write("Data in the Database actually :")
    st.dataframe(df_electricity)
    st.write("Electricity carbon intensity map :")
    dfs = px.data.gapminder().query("year==2007")
    fig = px.scatter_geo(dfs, locations="iso_alpha", color="continent",
                     hover_name="country", size="pop",
                     animation_frame="year",
                     projection="natural earth")
    st.plotly_chart(fig, use_container_width=True)

def get_electricity_data(client_connect: Connection):
    mydb = client_connect["electricity"]
    mycol = mydb["electricity_coll"]
    df_db = {"_id" : [],
            "_disclaimer" : [],
            "status" : [],
            "countryCode" : [],
            "data" : [],
            "units" : []}
    index = 0
    for x in mycol.find():
        df_db["_id"].insert(-1, index)
        df_db["_disclaimer"].insert(-1, x["_disclaimer"])
        df_db["status"].insert(-1, x["status"])
        df_db["countryCode"].insert(-1, x["countryCode"])
        df_db["data"].insert(-1, x["data"])
        df_db["units"].insert(-1, x["units"])
        index+=1
    return df_db
########## ########## ##########

########## TWITTER ##########
def display_twitter(client_connect):
    st.header("Twitter :")
    st.subheader("Simulation of Bitcoin mining output based on keyword\
        targeting by tweet location.")
    display_twitter_data(client_connect)

def display_twitter_data(client_connect: Connection):
    df_twitter = get_twitter_data(client_connect)
    st.write("Data in the Database actually :")
    st.dataframe(df_twitter)
    st.write("Twitter location pie :")
    dfs = px.data.tips()
    fig = px.pie(dfs, values='tip', names='day')
    st.plotly_chart(fig, use_container_width=True)

def get_twitter_data(client_connect: Connection):
    mydb = client_connect["twitter"]
    mycol = mydb["twitter_coll"]
    df_db = {"_id" : [],
            "created_at" : [],
            "text" : [],
            "user_screen_name" : [],
            "user_location" : []}
    index = 0
    for x in mycol.find():
        df_db["_id"].insert(-1, index)
        df_db["created_at"].insert(-1, x["created_at"])
        df_db["text"].insert(-1, x["text"])
        df_db["user_location"].insert(-1, x["user_location"])
        df_db["user_screen_name"].insert(-1, x["user_screen_name"])
        index+=1
    return df_db
########## ########## ##########

########## CRYPTO ##########
def display_crypto(conn):
    df = get_crypto_data(conn)
    id_max = df["index"].idxmax()
    real_time, desc = st.columns(2)
    min, moy, max = st.columns(3)
    nb_max = int(df['BTC_prices'].max())
    nb_moy = int(df["BTC_prices"].mean())
    nb_min = int(df["BTC_prices"].min())
    nb_real_time = int(df["BTC_prices"][id_max])

    real_time.metric("Real Time", str(nb_real_time) + " $")
    desc.markdown("**This is the Bitcoin value at the moment.\n \
        The value is refreshed all hours.**")
    min.metric("Minimum", str(nb_min) + " $")
    moy.metric("Mean", str(nb_moy) + " $")
    max.metric("Maximum", str(nb_max) + " $")
    display_crypto_data(conn)

def display_crypto_data(conn: Connection):
    df = get_crypto_data(conn)
    st.write("Data in the Database actually :")
    st.dataframe(df)
    st.write("Bitcoin price chart :")
    fig = px.line(df, x='BTC_timestamp', y='BTC_prices', markers=True)
    st.plotly_chart(fig, use_container_width=True)

def get_crypto_data(conn: Connection):
    df = pd.read_sql("SELECT * FROM bitcoin", con=conn)
    return df
########## ########## ##########

########## FRONT ##########
def display_header():
    st.title("Dashboard Project BDD")
    st.header("Crypto :")
    st.subheader("Description of the value of Bitcoin.")

def build_sidebar():
    with st.sidebar:
        st.title("Credits :")
        st.header("project produced by :")
        st.write("- **Mathieu Ly-Wa-Hoi**")
        st.write("- **BARRY Mamadou Djoulde**")
        st.write("- **Hugo Chantelot**")
########## ########## ##########

########## MAIN ##########
@st.cache(hash_funcs={Connection: id})
def get_connection(path: str):
    """Put the connection in cache to reuse if path does not change between Streamlit reruns.
    NB : https://stackoverflow.com/questions/48218065/programmingerror-sqlite-objects-created-in-a-thread-can-only-be-used-in-that-sa
    """
    return sqlite3.connect(path, check_same_thread=False)

if __name__ == "__main__":
    conn = get_connection(NAME_BDD)
    client_connect = MongoClient('mongo', 27017, username = 'root', password = 'root')
    build_sidebar()
    display_header()
    display_crypto(conn)
    display_twitter(client_connect)
    display_electricity(client_connect)
########## ########## ##########