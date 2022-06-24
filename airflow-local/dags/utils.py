import requests
import feedparser

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns

import os
from glob import glob

import requests
from bs4 import BeautifulSoup
from dateutil import parser

from datetime import datetime
from math import isnan

import spacy
from airflow.models import Variable

import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

import time
import random

MY_API_KEY = '5B3F9E5908006B10BDECF51F9A9A5A95'

def get_request(url,params=None):
    
    sleep_time = random.randint(1,10)
    try:
        response = requests.get(url=url,params=params)
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
        time.sleep(sleep_time)
        return get_request(url,params)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
        time.sleep(sleep_time)
        return get_request(url,params)
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
        time.sleep(sleep_time)
        return get_request(url,params)
    except Exception as e:
        print("Failed to get response cancelling...,",e)
        return 
        
    if response:
        return response.json()
    else:
        print("No Response. Retrying...")
        time.sleep(sleep_time)
        return get_request(url,params)

##initialize dfs
def get_appid(game_name):
    game_name = game_name.lower()
    response = requests.get(url=f'https://store.steampowered.com/search/?term={game_name}&category1=998', headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(response.text, 'html.parser')
    app_details = None
    app_id = None
    game_found = None
    try:
        app_details = soup.find(class_='search_result_row')
        app_id = app_details['data-ds-appid']
        game_found = app_details.find(class_="search_name").text.strip().lower()
    except:
        return None
    
    if game_name == game_found:
        return app_id
    return None

def date_published_tostr(published):
    return parser.parse(published).strftime("%Y-%m-%d %H:%M:%S")

def init_appids(df):
    df2 = df.copy()
    tags = df2["tags"].values
    source = df2["source"].iloc[0]
    appids = []
    for i,tag in enumerate(tags):
        try:
            if tag is None or isnan(tag):
                appids.append(None)
                print(f"{source}: game not found for:",i)
                continue
        except:
            pass

        ls_tag = eval(tag)
        for term in ls_tag:
            game_name = term["term"]
            appid = get_appid(game_name)
            if appid:
                appids.append(appid)
                print(f"{source}: YAY",appid,game_name)
                break
        else:
            appids.append(None)
            print(f"{source}: game not found for:",i)
    return appids

def init_null_appids(nouns):
    for noun in nouns:
        appid = get_appid(noun)
        print(f"checking if game \'{noun}\' on steam...")
        if appid:
            print(f"game \'{noun}\' found!.")
            print()
            return appid
    print(f"article has no games on steam.")
    print()
    return None



def get_nouns(title_summary):
    nlp = spacy.load("/model/en_core_web_sm/en_core_web_sm-3.3.0")
    stop_words = stopwords.words("english")
    
    summary = BeautifulSoup(title_summary,"html.parser").text.strip().lower()
    doc = nlp(summary)
    noun_phrases = set([noun.text for noun in doc.noun_chunks])
    return [noun for noun in noun_phrases if noun not in stop_words]

def upload_formatted_rss_feed(feed_name, feed):
    DATA_PATH = '/opt/airflow/data/'
    feed = feedparser.parse(feed)
    df = pd.DataFrame(feed['entries'])
    columns = ["title","title_detail","link","links","author","authors","published","published_parsed","tags","id","guidislink","summary","website_name"]
    df = df[columns]
    df["source"] = feed_name
    df = df_appids(df,feed_name)
    time_now = datetime.now().strftime('%Y-%m-%d_%I-%M-%S')
    filename = feed_name + '_' + time_now + '.csv'
    df.to_csv(f"{DATA_PATH}{filename}", index=False)
    print(f"df {filename} saved to .csv!")

def df_appids(df2,feed_name):
    df = df2.copy()

    appids = init_appids(df)
    print(f"getting appids for {feed_name}...")
    df["appids"] = appids
    df["summary"] = df["summary"].apply(lambda x: BeautifulSoup(x,"html.parser").text.strip().lower())
    df["title_summary"] = df["title"] +" "+ df["summary"]
    df["nouns"] = df["title_summary"].apply(get_nouns)
    df["date_str"] = df["published"].apply(date_published_tostr)
    df.loc[df["appids"].isnull(),"appids"]= df.loc[df["appids"].isnull(),"nouns"].apply(init_null_appids)
    print(f"appids for {feed_name} initialized!")
    return df