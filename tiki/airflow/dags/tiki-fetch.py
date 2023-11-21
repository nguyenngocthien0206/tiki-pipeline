import pandas as pd
import numpy as np
import requests
import time
import random
import pandas as pd
import json
import sys
import os
import pendulum
sys.path.append('/home/thien/tiki')

from read_write_json import read_json, write_json
from fetch_data import fetching, parameters

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'start_date': pendulum.datetime(2023,11,21,tz='UTC'),
    'owner': 'airflow'
}

dag = DAG('tiki-fetching', default_args=default_args, schedule_interval='@once',catchup=False)

crawl_products = PythonOperator(
    task_id='crawl_products',
    python_callable=fetching.fetch_products,
    op_kwargs= {
        'product_url': parameters.product_url,
        'product_headers': parameters.product_headers,
        'product_params': parameters.product_params,
        'product_cookies': parameters.product_cookies
    },
    dag=dag
)

crawl_authors = PythonOperator(
    task_id='crawl_authors',
    python_callable=fetching.fetch_authors,
    op_kwargs= {
        'author_url': parameters.author_url,
        'author_headers': parameters.author_headers,
        'author_params': parameters.author_params,
    },
    dag=dag
)

crawl_sellers = PythonOperator(
    task_id='crawl_sellers',
    python_callable=fetching.fetch_sellers,
    op_kwargs= {
        'author_url': parameters.seller_url,
        'author_headers': parameters.seller_headers,
        'author_params': parameters.seller_params,
    },
    dag=dag
)

crawl_reviews = PythonOperator(
    task_id='crawl_reviews',
    python_callable=fetching.fetch_reviews,
    op_kwargs= {
        'author_url': parameters.review_url,
        'author_headers': parameters.review_headers,
        'author_params': parameters.review_params,
    },
    dag=dag
)
    
crawl_products >> [crawl_authors, crawl_sellers] >> crawl_reviews


