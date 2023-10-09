import numpy as np
import pandas as pd
import sys
sys.path.append('/home/thien/tiki')
from read_write_json import read_json, write_json

def review_stg(reviews):
    review_lst = list()
    for item in reviews:
        review = dict()
        review['review_id'] = item['id']
        review['customer_id'] = item['customer_id']
        review['product_id'] = item['product_id']
        review['seller_product_id'] = item['spid']
        review['created_at'] = item['created_at']
        review['rating'] = item['rating']
        review['thank_count'] = item['thank_count']
        review['title'] = item['title']
        review['content'] = item['content']
        review_lst.append(review)
        
    df = pd.DataFrame(review_lst)
    df.to_csv('/home/thien/tiki/csv/reviews.csv',index=None)
    
def main():
    reviews = read_json('/home/thien/tiki/src/reviews.json')
    review_stg(reviews=reviews)
    
if __name__ == '__main__':
    main()