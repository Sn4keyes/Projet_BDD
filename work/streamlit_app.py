# streamlit_app.py

import streamlit as st
import pymongo

# Initialisation de la connection à MongoDB

client = pymongo.MongoClient('mongo', 27017, username = 'root', password = 'root')

# Collecte des données
# st.cache pour s'exécuter toutes les 10 minutes ou quand il y a du changement

def get_data():
    db = client.crypto
    items = db.crypto_col.find()
    items = list(items)  # créé un hashable pour st.cache
    return items


items = get_data()

# Résultats

for item in items:
    st.write(f"{item['BTC_prices']}")