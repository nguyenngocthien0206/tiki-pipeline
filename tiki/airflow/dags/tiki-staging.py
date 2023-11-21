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
import staging as stg
import create_table as qr
import insertion as ins

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

default_args = {
    'start_date': pendulum.datetime(2023,11,21,tz='UTC'),
    'owner': 'airflow'
}

dag = DAG('tiki-stg', start_date=pendulum.datetime(2023,11,21,tz='UTC'),schedule_interval='@once',catchup=False)

stg_author = PythonOperator(
    task_id='staging_author',
    python_callable=stg.author_stg,
    dag=dag
)

stg_customer = PythonOperator(
    task_id='staging_customer',
    python_callable=stg.customer_stg,
    dag=dag
)

stg_seller = PythonOperator(
    task_id='staging_seller',
    python_callable=stg.seller_stg,
    dag=dag
)

stg_product = PythonOperator(
    task_id='staging_product',
    python_callable=stg.product_stg,
    dag=dag
)

stg_written = PythonOperator(
    task_id='staging_written',
    python_callable=stg.written_stg,
    dag=dag
)

stg_review = PythonOperator(
    task_id='staging_review',
    python_callable=stg.review_stg,
    dag=dag
)

create_author_table = MySqlOperator(
    task_id='create_author_table',
    mysql_conn_id='tiki_conn',
    sql=qr.create_table_author,
    dag=dag
)

create_seller_table = MySqlOperator(
    task_id='create_seller_table',
    mysql_conn_id='tiki_conn',
    sql=qr.create_table_seller,
    dag=dag
)

create_customer_table = MySqlOperator(
    task_id='create_customer_table',
    mysql_conn_id='tiki_conn',
    sql=qr.create_table_customer,
    dag=dag
)

create_product_table = MySqlOperator(
    task_id='create_product_table',
    mysql_conn_id='tiki_conn',
    sql=qr.create_table_product,
    dag=dag
)

create_written_table = MySqlOperator(
    task_id='create_written_table',
    mysql_conn_id='tiki_conn',
    sql=qr.create_table_written,
    dag=dag
)

create_review_table = MySqlOperator(
    task_id='create_review_table',
    mysql_conn_id='tiki_conn',
    sql=qr.create_table_review,
    dag=dag
)

create_category_table = MySqlOperator(
    task_id='create_category_table',
    mysql_conn_id='tiki_conn',
    sql=qr.create_table_category,
    dag=dag
)

add_fk_product_seller = MySqlOperator(
    task_id='add_fk_product_seller',
    mysql_conn_id='tiki_conn',
    sql=qr.add_fk_product_seller,
    dag=dag
)

add_fk_product_category = MySqlOperator(
    task_id='add_fk_product_category',
    mysql_conn_id='tiki_conn',
    sql=qr.add_fk_product_category,
    dag=dag
)

add_fk_written_product = MySqlOperator(
    task_id='add_fk_written_product',
    mysql_conn_id='tiki_conn',
    sql=qr.add_fk_written_product,
    dag=dag
)

add_fk_written_author = MySqlOperator(
    task_id='add_fk_written_author',
    mysql_conn_id='tiki_conn',
    sql=qr.add_fk_written_author,
    dag=dag
)

add_fk_review_product = MySqlOperator(
    task_id='add_fk_review_product',
    mysql_conn_id='tiki_conn',
    sql=qr.add_fk_review_product,
    dag=dag
)

add_fk_review_customer = MySqlOperator(
    task_id='add_fk_review_customer',
    mysql_conn_id='tiki_conn',
    sql=qr.add_fk_review_customer,
    dag=dag
)

insert_author = PythonOperator(
    task_id='insert_author',
    python_callable=ins.insert_author,
    dag=dag
)

insert_seller = PythonOperator(
    task_id='insert_seller',
    python_callable=ins.insert_seller,
    dag=dag
)

insert_category = PythonOperator(
    task_id='insert_category',
    python_callable=ins.insert_category,
    dag=dag
)

insert_customer = PythonOperator(
    task_id='insert_customer',
    python_callable=ins.insert_customer,
    dag=dag
)

insert_product = PythonOperator(
    task_id='insert_product',
    python_callable=ins.insert_product,
    dag=dag
)

insert_written = PythonOperator(
    task_id='insert_written',
    python_callable=ins.insert_written,
    dag=dag
)

insert_review = PythonOperator(
    task_id='insert_review',
    python_callable=ins.insert_review,
    dag=dag
)

[stg_author, stg_seller, stg_customer] >> stg_product >> stg_written >> stg_review
stg_review >> [create_author_table,create_seller_table,create_customer_table,create_category_table,create_product_table,create_written_table,create_review_table] >> add_fk_product_seller
add_fk_product_seller >> [add_fk_product_category,add_fk_written_product,add_fk_written_author,add_fk_review_customer,add_fk_review_product]
add_fk_review_product >> [insert_author,insert_seller,insert_category,insert_customer] >> insert_product >> insert_written >> insert_review







