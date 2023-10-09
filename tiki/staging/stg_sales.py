import numpy as np
import pandas as pd
import sys
sys.path.append('/home/thien/tiki')
from read_write_json import read_json, write_json

def sales_stg(reviews):
    sales_lst = list()
    i = 1
    for item in reviews:
        sale = dict()
        sale['sales_id'] = i
        sale['customer_id'] = item['customer_id']
        sale['product_id'] = item['product_id']
        sale['seller_product_id'] = item['spid']
        created_by = item['created_by']
        if created_by != None:
            sale['purchased_at'] = created_by['purchased_at']
        sales_lst.append(sale)
        i = i + 1
        
    df = pd.DataFrame(sales_lst)
    df.to_csv('/home/thien/tiki/csv/sales.csv',index=None)
    
def main():
    reviews = read_json('/home/thien/tiki/src/reviews.json')
    sales_stg(reviews=reviews)
    
if __name__ == '__main__':
    main()