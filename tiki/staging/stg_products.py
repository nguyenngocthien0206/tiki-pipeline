import numpy as np
import pandas as pd
import sys
sys.path.append('/home/thien/tiki')
from read_write_json import read_json, write_json

def prod_stg(products):
    df_prod = list()
    for item in products:
        prod = dict()
        prod['product_id'] = item['id']
        prod['product_name'] = item['name']
        prod['original_price'] = item['original_price']
        prod['discount_rate'] = item['discount_rate']
        prod['discount'] = item['discount']
        prod['price'] = item['price']
        prod['quantity_sold'] = item['visible_impression_info']['amplitude']['all_time_quantity_sold']
        prod['rating_average'] = item['rating_average']
        prod['review_count'] = item['review_count']
        prod['seller_id'] = item['seller_id']
        prod['seller_type'] = item['visible_impression_info']['amplitude']['seller_type']
        prod['seller_product_id'] = item['visible_impression_info']['amplitude']['seller_product_id']
        prod['seller_product_sku'] = item['visible_impression_info']['amplitude']['seller_product_sku']
        prod['primary_category_name'] = item['visible_impression_info']['amplitude']['primary_category_name']
        df_prod.append(prod)

    df = pd.DataFrame(df_prod)
    df.to_csv('/home/thien/tiki/csv/products.csv',index=None)
    
def main():
    products = read_json('/home/thien/tiki/src/products.json')
    prod_stg(products=products)
    
if __name__ == '__main__':
    main()