from datetime import timedelta
import boto3
import os
from io import StringIO, BytesIO
import feedparser
import pandas as pd
import spacy
import requests
import pybase64
from google.cloud import bigquery, storage
import shutil 


# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.decorators import task
# from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
# from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator
from datetime import datetime

import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from utils import init_appids,init_null_appids,get_nouns,date_published_tostr,upload_formatted_rss_feed

BUCKET_NAME = "news_sites"

# Group directory in the bucket
MY_FOLDER_PREFIX = "fem_hans"

# Data directory for CSVs and OSM Images
DATA_PATH = '/opt/airflow/data/'

@task(task_id="kotaku")
def kotaku_feed(ds=None, **kwargs):
    upload_formatted_rss_feed("kotaku","https://kotaku.com/rss")

@task(task_id="escapist_mag")
def escapist_mag_feed(ds=None, **kwargs):
    upload_formatted_rss_feed("escapist_mag","https://www.escapistmagazine.com/v2/feed/")

@task(task_id="eurogamer")
def eurogamer_feed(ds=None, **kwargs):
    upload_formatted_rss_feed("eurogamer","https://www.eurogamer.net/?format=rss")


@task(task_id="gamespot_standard")
def gamespot_feed(ds=None, **kwargs):
    upload_formatted_rss_feed("gamespot","https://www.gamespot.com/feeds/mashup/")

@task(task_id="indigames_plus")
def indigames_plus_feed(ds=None, **kwargs):
    upload_formatted_rss_feed("indigames_plus","https://indiegamesplus.com/feed")

@task(task_id="ps_blog")
def ps_blog_feed(ds=None, **kwargs):
    upload_formatted_rss_feed("ps_blog","http://feeds.feedburner.com/psblog")


@task(task_id="rock_paper_sg")
def rock_paper_sg_feed(ds=None, **kwargs):
    upload_formatted_rss_feed("rock_paper_sg","http://feeds.feedburner.com/RockPaperShotgun")

@task(task_id="steam_news")
def steam_news_feed(ds=None, **kwargs):
    upload_formatted_rss_feed("steam_news","https://store.steampowered.com/feeds/news.xml")

@task(task_id="ancient_gaming")
def ancient_gaming_feed(ds=None, **kwargs):
    upload_formatted_rss_feed("ancient_gaming","http://feeds.feedburner.com/TheAncientGamingNoob")



