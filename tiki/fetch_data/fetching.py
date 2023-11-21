import requests
import time
import random
import pandas as pd
import json
import sys
sys.path.append('/home/thien/tiki')
from read_write_json import read_json, write_json
import math

def fetch_products(**kwargs):
    products = list()

    for i in range(1,51):
        kwargs['product_params']['page'] = i
        response = requests.get(kwargs['product_url'], headers=kwargs['product_headers'], params=kwargs['product_params'], cookies=kwargs['product_cookies'])
        if response.status_code == 200:
            print('Request Success!: Page {}'.format(i))
            for record in response.json().get('data'):
                d = dict()
                d['id'] = record.get('id')
                d['name'] = record.get('name')
                d['has_ebook'] = record.get('has_ebook')
                d['original_price'] = record.get('original_price')
                d['discount_rate'] = record.get('discount_rate')
                d['discount'] = record.get('discount')
                d['price'] = record.get('price')
                d['quantity_sold'] = record.get('quantity_sold')
                d['rating_average'] = record.get('rating_average')
                d['review_count'] = record.get('review_count')
                d['shippable'] = record.get('shippable')
                d['seller_id'] = record.get('seller_id')
                d['visible_impression_info'] = record.get('visible_impression_info')
                d['spid'] = record.get('seller_product_id')
                d['url'] = 'https://tiki.vn/' + record.get('url_path')
                products.append(d)
    
    write_json('/home/thien/tiki/src/products.json', products)
    
def fetch_authors(**kwargs):
    products = read_json('/home/thien/tiki/src/products.json')
    pid_list = [(item['id'],item['spid']) for item in products]
    author_list = list()
    # fetch authors
    i = 1
    
    for item in pid_list:
        kwargs['author_params']['page'] = item[1]
        response = requests.get(kwargs['author_url'].format(item[0]), headers=kwargs['author_headers'], params=kwargs['author_params'])
        if response.status_code == 200 and 'application/json' in response.headers['Content-Type']:
            print('Request: {}, PID: {} - SPID {}'.format(i,item[0],item[1]))
            authors = response.json().get('authors')
            if authors != None:
                for author in authors:
                    author['pid'] = item[0]
                    author_list.append(author)
        i = i + 1    
        
    write_json('/home/thien/tiki/src/authors.json',author_list)
    
def fetch_sellers(**kwargs):
    products = read_json('/home/thien/tiki/src/products.json')
    pid_list = [(item['id'],item['spid']) for item in products]
    seller_list = list()
    i = 1
    
    for item in pid_list:
        kwargs['seller_params']['spid'] = item[1]
        response = requests.get(kwargs['seller_url'].format(item[0]), headers=kwargs['seller_headers'], params=kwargs['seller_params'])
        if response.status_code == 200 and 'application/json' in response.headers['Content-Type']:
            print('Request: {}, PID: {} - SPID {}'.format(i,item[0],item[1]))
            other_sellers = response.json().get('other_sellers')
            current_seller = response.json().get('current_seller')
                
            if len(other_sellers) != 0:
                for other_seller in other_sellers:
                    other_seller['pid'] = item[0]
                    
                    seller_list.append(other_seller)
            if current_seller != None:
                current_seller['pid'] = item[0]
                seller_list.append(current_seller)
        i = i + 1    
        write_json('/home/thien/tiki/src/sellers.json',seller_list)
        
def fetch_reviews(**kwargs):
    products = read_json('/home/thien/tiki/src/products.json')
    pid_list = [(item['id'],item['spid'],item['seller_id'],item['review_count']) for item in products]
    
    review_list = list()
    
    try:
        for item in pid_list:
            kwargs['review_params']['product_id'] = item[0]
            kwargs['review_params']['spid'] = item[1]
            kwargs['review_params']['seller_id'] = item[2]
            max_page = math.ceil(item[3]/20) # 5 is default limit in params, 20 is max.
            
            for i in range(1,max_page + 1):
                kwargs['review_params']['page'] = i
                response = requests.get(kwargs['review_url'], headers=kwargs['review_headers'], params=kwargs['review_params'])
                if response.status_code == 200 and 'application/json' in response.headers['Content-Type']:
                    print('Request Success: PID - {}, SPID - {}, SELLER_ID - {}, PAGE - {}'.format(kwargs['review_params']['product_id'],kwargs['review_params']['spid'],kwargs['review_params']['seller_id'],kwargs['review_params']['page']))
                    reviews = response.json().get('data')
                    
                    for review in reviews:
                        review['pid'] = item[0]
                        review['spid'] = item[1]
                        review['seller_id'] = item[2]
                        review_list.append(review)
            
    except:
        write_json('/home/thien/tiki/src/reviews.json',review_list)
    
    write_json('/home/thien/tiki/src/reviews.json',review_list)