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
    'X-Guest-Token': '1HdCx3rAeKhulntzMOLSjD9sIPXfYy8m',
    'Content-Type': 'application/json; charset=utf-8',
    'Via': 'kong/2.1.4, 1.1 google',
    'X-Content-Type-Options': 'nosniff'
}

params = {
    'platform': 'web',
    'spid': 35191894,
    'version': 3
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
    'cto_bundle': 'VjrYmV9KNW83RG1LV0lMSXklMkZBWUV6RTBMaWhjbHkyYmlYUlElMkYxWSUyRmEyVGxXczZhdzMlMkJPeEslMkZEb3VRaU9PNzZWc3dMRHlIV1NOMUVLQkdJU1VUa3Q4T2dDWGxkV0ZyUTBXS3FsMkppeHE2TktTNzR0b1ltV2RKTmFScUtnc3BRMHpmWlg3czNZVWtFMXBTdVZHZSUyQkdNakRVZ1d0bjdRa0E2eGxEak9qTHlmN0xtS3hMdzVkQUFPbSUyQjNaTElRNlBxdTVOZnk4cmolMkZ0Q3lkN3BONmxUelpWM2EyZyUzRCUzRA',
    '_ga_W6PZ1YEX5L': 'GS1.1.1695632207.1.1.1695632330.0.0.0',
    'delivery_zone': 'Vk4wNTcwMTcwMDg',
    '_gid': 'GA1.2.232530208.1695715641',
    'tiki_client_id': '783889012.1687873037',
    '_ga': 'GA1.1.783889012.1687873037',
    'amp_99d374': '4nIMmsIMG_re_61dhj9aKp...1hb88ef7q.1hb88rit9.b7.cl.ns',
    '_ga_GSD4ETCY1D': 'GS1.1.1695715644.6.1.1695716109.60.0.0'
}

def fetch_sellers(headers, params):
    products = read_json('/home/thien/tiki/src/products.json')
    pid_list = [(item['id'],item['spid']) for item in products]
    author_list = list()
    seller_list = list()
    # fetch authors
    i = 1
    
    for item in pid_list:
        params['spid'] = item[1]
        response = requests.get('https://tiki.vn/api/v2/products/{}?'.format(item[0]), headers=headers, params=params)
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
    return seller_list

def main():
    sellers = fetch_sellers(headers, params)
    write_json('/home/thien/tiki/src/sellers.json',sellers)
    
if __name__ == '__main__':
    main()
