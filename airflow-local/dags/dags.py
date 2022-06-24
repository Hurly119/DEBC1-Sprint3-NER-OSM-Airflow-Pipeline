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

from utils import upload_formatted_rss_feed,scrape_reviews,scrape_appdetails

BUCKET_NAME = "news_sites"

# Group directory in the bucket
MY_FOLDER_PREFIX = "fem_hans"

DATE_NOW = datetime.now().strftime("%Y-%m-%d")
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


@task(task_id="combine_all_articles")
def combine_all_articles(ds=None,**kwargs):
    files = os.listdir(DATA_PATH)
    dfs = []
    for file in files:
        outfile = f"{DATA_PATH}{file}"
        if not outfile.endswith('.csv'):
            continue
        df = pd.read_csv(outfile)
        dfs.append(df)
    game_articles = pd.concat(dfs)

    game_articles.to_csv(f"{DATA_PATH}game_articles_{DATE_NOW}.csv")

@task(task_id="scrape_game_reviews")
def scrape_game_reviews(ds=None,**kwargs):
    appids = pd.read_csv(f"{DATA_PATH}game_articles_{DATE_NOW}.csv")["appids"].unique()
    all_reviews = []
    for appid in appids:
        reviews_list = scrape_reviews(appid)
        all_reviews += reviews_list
    game_reviews = pd.DataFrame(all_reviews)

    game_reviews.to_csv("f{DATA_PATH}game_reviews_{DATE_NOW}.csv")

@task(task_id="scrape_game_details")
def scrape_game_details(ds=None,**kwargs):
    appids = pd.read_csv(f"{DATA_PATH}game_articles_{DATE_NOW}.csv")["appids"].unique()
    
    lst_game_details = scrape_appdetails(appids)
    
    game_details = pd.DataFrame(lst_game_details)

    game_details.to_csv("f{DATA_PATH}game_details_{DATE_NOW}.csv")


with DAG(
    'scrapers_proj_test',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        # 'depends_on_past': False,
        # 'email': ['caleb@eskwelabs.com'],
        # 'email_on_failure': False,
        # 'email_on_retry': False,
        # 'retries': 1,
        # 'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='Pipeline demo',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 15),
    catchup=False,
    tags=['scrapers'],
) as dag:

    t1 = BashOperator(
        task_id="t1_start_msg",
        bash_command = "echo 't1_end'",
        # bash_command="curl -X POST -H 'Content-type: application/json' --data '{\"text\" : \"starting task 1: scraping\"}' \"https://discord.com/api/webhooks/986224448984195082/pQp4GNcVWh-J2XtmIycVnjxYuGRGVIYFeveDRS5EwvgmGozthyd_alj8wbeKhfVn9SSk/slack\"",
        dag=dag
    )

    t1_end = BashOperator(
        task_id="t1_end_msg",
        bash_command = "echo 't1_end'",
        # bash_command="curl -X POST -H 'Content-type: application/json' --data '{\"text\" : \"ending task 1: scraping\"}' \"https://discord.com/api/webhooks/986224448984195082/pQp4GNcVWh-J2XtmIycVnjxYuGRGVIYFeveDRS5EwvgmGozthyd_alj8wbeKhfVn9SSk/slack\"",
        dag=dag
    )

    t1 >> [kotaku_feed(),indigames_plus_feed()] >> [scrape_game_details(),scrape_game_reviews()] >> t1_end