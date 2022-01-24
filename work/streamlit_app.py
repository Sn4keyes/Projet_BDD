from sqlite3 import Connection
from pathlib import Path
import streamlit as st
import altair as alt
import pandas as pd
import numpy as np
import sqlite3
import plotly.express as px

NAME_BDD = "crypto.db"

########## ELECTRICITY ##########
def display_electricity():
    st.header("Electricity :")
    st.subheader("Simulation of the correlation between Bitcoin mining and the\
        carbon intensity of Bitcoin mining countries.")

        
    df = px.data.gapminder().query("year==2007")
    st.write(df)
    fig = px.scatter_geo(df, locations="iso_alpha", color="continent",
                        hover_name="country", size="pop",
                        projection="natural earth")
    st.plotly_chart(fig, use_container_width=True)
########## ########## ##########

########## TWITTER ##########
def display_twitter():
    df = get_data(conn)

    st.header("Twitter :")
    st.subheader("Simulation of Bitcoin mining output based on keyword\
        targeting by tweet location.")
    
    # This dataframe has 244 lines, but 4 distinct values for `day`
    dfs = px.data.tips()
    fig = px.pie(dfs, values='tip', names='day')
    st.plotly_chart(fig, use_container_width=True)
########## ########## ##########

########## CRYPTO ##########
def display_crypto():
    df = get_data(conn)
    id_max = df["BTC_prices"].idxmax()
    real_time, desc = st.columns(2)
    min, moy, max = st.columns(3)
    nb_max = int(df['BTC_prices'].max())
    nb_moy = int(df["BTC_prices"].mean())
    nb_min = int(df["BTC_prices"].min())
    nb_real_time = int(df["BTC_prices"][id_max])

    real_time.metric("Real Time", str(nb_real_time) + " $")
    desc.markdown("**This is the Bitcoin value at the moment.\n \
        The value is refreshed all hours.**")
    crypto_period = st.selectbox(
        'choose a period',
        ('1 Day', '1 Week', '1 Month'))
    min.metric("Minimum", str(nb_min) + " $")
    moy.metric("Mean", str(nb_moy) + " $")
    max.metric("Maximum", str(nb_max) + " $")
    display_good_period(crypto_period)

def display_good_period(crypto_period):
    if crypto_period == "1 Day":
        st.write("**1 Day choose**")
    elif crypto_period == "1 Week":
        st.write("**1 Week choose**")
    else:
        st.write("**1 Month choose**")
########## ########## ##########

########## FRONT ##########
def display_header():
    st.title("Dashboard Project BDD")
    st.header("Crypto :")
    st.subheader("Description of the value of Bitcoin.")

def build_sidebar(conn: Connection):
    with st.sidebar:
        st.title("Credits :")
        st.header("project produced by :")
        st.write("- **Mathieu Ly-Wa-Hoi**")
        st.write("- **BARRY Mamadou Djoulde**")
        st.write("- **Hugo Chantelot**")
########## ########## ##########

########## MAIN ##########
def display_data(conn: Connection):
    st.dataframe(get_data(conn))

    df = get_data(conn)
    df2 = pd.DataFrame(
        np.random.randn(200, 3),
        columns=['a', 'b', 'c'])
    c = alt.Chart(df).mark_bar().encode(
        x='BTC_timestamp', y='BTC_prices', size='BTC_prices').properties(width=1000)
    st.altair_chart(c, use_container_width=True)
    st.line_chart(df['BTC_prices'])


def get_data(conn: Connection):
    df = pd.read_sql("SELECT * FROM bitcoin", con=conn)
    return df

@st.cache(hash_funcs={Connection: id})
def get_connection(path: str):
    """Put the connection in cache to reuse if path does not change between Streamlit reruns.
    NB : https://stackoverflow.com/questions/48218065/programmingerror-sqlite-objects-created-in-a-thread-can-only-be-used-in-that-sa
    """
    return sqlite3.connect(path, check_same_thread=False)

if __name__ == "__main__":
    conn = get_connection(NAME_BDD)
    build_sidebar(conn)
    display_header()
    display_crypto()
    display_data(conn)
    display_twitter()
    display_electricity()
########## ########## ##########