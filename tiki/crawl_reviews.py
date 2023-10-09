import requests
import time
import random
import pandas as pd
import json
from read_write_json import read_json, write_json
import math

headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,vi-VN;q=0.8,vi;q=0.7,fr-FR;q=0.6,fr;q=0.5',
    'Referrer': 'https://tiki.vn/nha-sach-tiki/c8322',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
    'X-Guest-Token': '1HdCx3rAeKhulntzMOLSjD9sIPXfYy8m',
    'Content-Type': 'application/json; charset=utf-8',
    'Via': 'kong/2.1.4, 1.1 google',
    'X-Content-Type-Options': 'nosniff'
}

params = {
    'limit': 20,
    'include': 'comments,contribute_info,attribute_vote_summary',
    'sort': 'score|desc,id|desc,stars|all',
    'page': 1,
    'spid': 187827005,
    'product_id': 187827003,
    'seller_id': 1
}

def fetch_reviews(headers, params):
    products = read_json('/home/thien/tiki/src/products.json')
    pid_list = [(item['id'],item['spid'],item['seller_id'],item['review_count']) for item in products]
    
    review_list = list()
    
    try:
        for item in pid_list:
            params['product_id'] = item[0]
            params['spid'] = item[1]
            params['seller_id'] = item[2]
            max_page = math.ceil(item[3]/20) # 5 is default limit in params, 20 is max.
            
            for i in range(1,max_page + 1):
                params['page'] = i
                response = requests.get('https://tiki.vn/api/v2/reviews?', headers=headers, params=params)
                if response.status_code == 200 and 'application/json' in response.headers['Content-Type']:
                    print('Request Success: PID - {}, SPID - {}, SELLER_ID - {}, PAGE - {}'.format(params['product_id'],params['spid'],params['seller_id'],params['page']))
                    reviews = response.json().get('data')
                    
                    for review in reviews:
                        review['pid'] = item[0]
                        review['spid'] = item[1]
                        review['seller_id'] = item[2]
                        review_list.append(review)
            
    except:
        write_json('/home/thien/tiki/src/reviews.json',review_list)
    
    return review_list


def main():
    reviews = fetch_reviews(headers, params)
    write_json('/home/thien/tiki/src/reviews.json',reviews)
    
if __name__ == '__main__':
    main()