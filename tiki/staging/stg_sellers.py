import numpy as np
import pandas as pd
import sys
sys.path.append('/home/thien/tiki')
from read_write_json import read_json, write_json

def seller_stg(sellers):
    sellers_set = set()
    for item in sellers:
        seller = (item['id'],item['name'],item['link'])
        sellers_set.add(seller)
        
    df = pd.DataFrame(sellers_set, columns=['seller_id', 'seller_name', 'seller_link'])

    df.to_csv('/home/thien/tiki/csv/sellers.csv',index=None)
    
def main():
    sellers = read_json('/home/thien/tiki/src/sellers.json')
    seller_stg(sellers=sellers)
    
if __name__ == '__main__':
    main()