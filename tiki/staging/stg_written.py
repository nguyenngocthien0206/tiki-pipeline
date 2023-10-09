import numpy as np
import pandas as pd
import sys
sys.path.append('/home/thien/tiki')
from read_write_json import read_json, write_json

def author_stg(auth):
    authors = set()
    for item in auth:
        author = (item['id'],item['pid'])
        authors.add(author)
        
    df = pd.DataFrame(authors, columns=['author_id', 'product_id'])

    df.to_csv('/home/thien/tiki/csv/written.csv',index=None)
    
def main():
    authors = read_json('/home/thien/tiki/src/authors.json')
    author_stg(auth=authors)
    
if __name__ == '__main__':
    main()