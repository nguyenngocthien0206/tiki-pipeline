import numpy as np
import pandas as pd
import sys
sys.path.append('/home/thien/tiki')
from read_write_json import read_json, write_json

def customer_stg(reviews):
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
    df.to_csv('/home/thien/tiki/csv/customers.csv',index=None)
    
def main():
    reviews = read_json('/home/thien/tiki/src/reviews.json')
    customer_stg(reviews=reviews)
    
if __name__ == '__main__':
    main()