import numpy as np
import pandas as pd
import sys
sys.path.append('/home/thien/tiki')
from read_write_json import read_json, write_json
import datetime

def author_stg():
    auth = read_json('/home/thien/tiki/src/authors.json')
    authors = set()
    for item in auth:
        author = (item['id'],item['name'])
        authors.add(author)
        
    df = pd.DataFrame(authors, columns=['author_id', 'author_name'])
    df = df.drop(df[df["author_id"].duplicated() == True].index)

    df.to_csv('/home/thien/tiki/csv/authors.csv',index=None)
    
def customer_stg():
    reviews = read_json('/home/thien/tiki/src/reviews.json')
    customer_lst = set()
    for item in reviews:
        customer_info = item['created_by']
        if customer_info != None:
            customer = (customer_info['id'],customer_info['name'],customer_info['region'],customer_info['avatar_url'])
            
            if 'created_time' in customer_info:
                customer = customer + tuple([customer_info['created_time']])
            else:
                customer = customer + tuple([""])
        
        customer_lst.add(customer)
            # if customer_info['created_time'] != None:
            #     customer['created_time'] = customer_info['created_time']
            # else:
            #     customer['created_time'] = None
        
    df = pd.DataFrame(customer_lst,columns=['customer_id','customer_name','region','avatar_url','created_time'])
    df = df.drop(df[df['customer_id'].isnull() == True].index) # Xóa index = null
    # TH trùng id (created_at null hoặc ko):
    list_duplicate_id = df[(df["customer_id"].duplicated() == True)]['customer_id']
    df = df.drop(df[df['customer_id'].isin(list_duplicate_id) & (df["created_time"].isna() == True)].index) # Xóa trùng & created_at là null
    
    df = df.drop(df[df['customer_id'].duplicated() == True].index) # Xóa các TH trùng id còn lại

    # Xử lý name là null:
    len_No_name = len(df[df["customer_name"].isnull() == True])
    replacement_name = ['Unnamed' + str(i) for i in range(len_No_name)]

    index_no_name, tmp = df[df["customer_name"].isnull() == True].index, 0
    for i in index_no_name:
        df.loc[i, 'customer_name'] = replacement_name[tmp]
        tmp += 1

    df = df.drop(df[df['created_time'] == '0000-00-00 00:00:00'].index)
    df.to_csv('/home/thien/tiki/csv/customers.csv',index=None)
    
def product_stg():
    products = read_json('/home/thien/tiki/src/products.json')
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
    df = df.drop(df[df['product_id'].duplicated() == True].index)
    df = df.drop(['discount_rate', 'discount', 'price'], axis=1)

    df_seller = pd.read_csv("/home/thien/tiki/csv/sellers.csv")
    df = df[df['seller_id'].isin(df_seller['seller_id'])]
    col = sorted(df['primary_category_name'].unique())
    arr_index = list(range(1, len(col) + 1))
    new_data = pd.DataFrame({'category_id': arr_index, 'category': col})
    new_data.to_csv('/home/thien/tiki/csv/categories.csv', index=False)
    
    dictionary = dict(zip(col, arr_index))
    df['primary_category_name'] = df['primary_category_name'].map(dictionary)
    df = df.rename(columns={'primary_category_name': 'category_id'})
    df.to_csv('/home/thien/tiki/csv/products.csv',index=False)
    
def review_stg():
    reviews = read_json('/home/thien/tiki/src/reviews.json')
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
    df = df.drop(df[df.duplicated() == True].index)

    data_sort = df[df['review_id'].isin(df[df['review_id'].duplicated() == True].review_id)].sort_values(by=['review_id', 'thank_count'], ascending = False)
    df = df.drop(data_sort[data_sort['review_id'].duplicated() == True].index)

    data_product = pd.read_csv("/home/thien/tiki/csv/products.csv")
    df = df[df['product_id'].isin(data_product['product_id'])]
    data_customer = pd.read_csv("/home/thien/tiki/csv/customers.csv")
    df = df[df['customer_id'].isin(data_customer['customer_id'])]

    df['created_at'] = df['created_at'].apply(lambda x: datetime.datetime.fromtimestamp(x))
    df.to_csv('/home/thien/tiki/csv/reviews.csv',index=None)
    
def seller_stg():
    sellers = read_json('/home/thien/tiki/src/sellers.json')
    sellers_set = set()
    for item in sellers:
        seller = (item['id'],item['name'],item['link'])
        sellers_set.add(seller)
        
    df = pd.DataFrame(sellers_set, columns=['seller_id', 'seller_name', 'seller_link'])

    df.to_csv('/home/thien/tiki/csv/sellers.csv',index=None)
    
def written_stg():
    auth = read_json('/home/thien/tiki/src/authors.json')
    authors = set()
    for item in auth:
        author = (item['id'],item['pid'])
        authors.add(author)
        
    df = pd.DataFrame(authors, columns=['author_id', 'product_id'])
    data_product = pd.read_csv("/home/thien/tiki/csv/products.csv")
    df = df[df['product_id'].isin(data_product['product_id'])]

    data_author = pd.read_csv("/home/thien/tiki/csv/authors.csv")
    df = df[df['author_id'].isin(data_author['author_id'])]

    df.to_csv('/home/thien/tiki/csv/written.csv',index=None)