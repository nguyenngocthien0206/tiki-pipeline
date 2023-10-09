import requests
import time
import random
import pandas as pd
import json
from read_write_json import read_json, write_json

headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,vi-VN;q=0.8,vi;q=0.7,fr-FR;q=0.6,fr;q=0.5',
    'Referrer': 'https://tiki.vn/nha-sach-tiki/c8322',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
    'X-Guest-Token': '1HdCx3rAeKhulntzMOLSjD9sIPXfYy8m'
}

params = {
    'limit': 40,
    'include': 'advertisement',
    'aggregations': 2,
    'version': 'home-persionalized',
    'trackity_id': 'c383ed8d-48fa-396b-5bc7-3e3066bb76b1',
    'category': 8322,
    'page': 1,
    'urlKey': 'nha-sach-tiki'
}

cookies = {
    '_hjSessionUser_522327': 'eyJpZCI6IjgwMTFjOWZhLWY2MjEtNWU2Ny05YzUyLTA0ZDdhNmRlMTM0NyIsImNyZWF0ZWQiOjE2NTY5MzYzNzk0NzYsImV4aXN0aW5nIjp0cnVlfQ',
    '_trackity': 'c383ed8d-48fa-396b-5bc7-3e3066bb76b1	',
    '_gcl_au': '1.1.420614684.1690006125',
    'TOKENS': '{%22access_token%22:%221HdCx3rAeKhulntzMOLSjD9sIPXfYy8m%22}',
    '_ga_KNZCHDVZCP': 'GS1.1.1694674713.2.1.1694674740.0.0.0',
    'TIKI_RECOMMENDATION': 'a19e3f0f239574461319d0c36d67cc8e',
    'TKSESSID': 'f9405c62a7709ca78972407d906cf3ff',
    '_bs': 'e278b40b-788e-8fa9-429b-e17c409b52be	',
    'cto_bundle': '7gGMZl9KNW83RG1LV0lMSXklMkZBWUV6RTBMaXNaRDlVUGVOdGFzdnByMVRiMmh2WFVDdXZLclUxbkJmQjQ3QWF6UmtQNFQ0ZzZiZE1DU0Nlb2JjWDBOejh2WjNvcFRjcGVPa0l5Uk9PS0NzdGlyVUhIQ2dQWmhjJTJGS2hOVjl4JTJCcHhQanpvazA1SFd4Rzl3Mk1USVNPbVZmaUNGYUZTYmFLSUZEVHRiWktoJTJGWnRzZFpDUjR5cjQlMkJKS3dmQ0lrQ2l4SUpLeVIxNUFKS3FDVmJIdlBCWm40U0FVdmRlUSUzRCUzRA',
    '_ga_W6PZ1YEX5L': 'GS1.1.1695632207.1.1.1695632330.0.0.0',
    'delivery_zone': 'Vk4wNTcwMTcwMDg',
    '_gid': 'GA1.2.232530208.1695715641',
    'tiki_client_id': '783889012.1687873037',
    '_ga': 'GA1.1.783889012.1687873037',
    'amp_99d374': '4nIMmsIMG_re_61dhj9aKp...1hb88ef7q.1hb88rit9.b7.cl.ns',
    '_ga_GSD4ETCY1D': 'GS1.1.1695715644.6.1.1695716109.60.0.0'
}

def parse_res(record):
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
    
    return d

def fetch_products(url, headers, params, cookies):
    products = list()

    for i in range(1,51):
        params['page'] = i
        response = requests.get(url, headers=headers, params=params, cookies=cookies)
        if response.status_code == 200:
            print('Request Success!: Page {}'.format(i))
            for record in response.json().get('data'):
                obj = parse_res(record)
                products.append(obj)
    
    return products

def main():
    product_list = fetch_products('https://tiki.vn/api/personalish/v1/blocks/listings?', headers, params, cookies)
    write_json('/home/thien/tiki/src/products.json',product_list)
    
if __name__ == '__main__':
    main()