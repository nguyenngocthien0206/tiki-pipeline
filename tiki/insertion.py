from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

def insert_author():
    hook = MySqlHook(mysql_conn_id='tiki_conn', local_infile=True)
    hook.bulk_load(table='author', tmp_file='/home/thien/tiki/csv/authors.csv')
    
def insert_seller():
    hook = MySqlHook(mysql_conn_id='tiki_conn', local_infile=True)
    hook.bulk_load(table='seller', tmp_file='/home/thien/tiki/csv/sellers.csv')
    
def insert_category():
    hook = MySqlHook(mysql_conn_id='tiki_conn', local_infile=True)
    hook.bulk_load(table='category', tmp_file='/home/thien/tiki/csv/categories.csv')
    
def insert_customer():
    hook = MySqlHook(mysql_conn_id='tiki_conn', local_infile=True)
    hook.bulk_load(table='customer', tmp_file='/home/thien/tiki/csv/customers.csv')
    
def insert_product():
    hook = MySqlHook(mysql_conn_id='tiki_conn', local_infile=True)
    hook.bulk_load(table='product', tmp_file='/home/thien/tiki/csv/products.csv')
    
def insert_written():
    hook = MySqlHook(mysql_conn_id='tiki_conn', local_infile=True)
    hook.bulk_load(table='written', tmp_file='/home/thien/tiki/csv/written.csv')
    
def insert_review():
    hook = MySqlHook(mysql_conn_id='tiki_conn', local_infile=True)
    hook.bulk_load(table='review', tmp_file='/home/thien/tiki/csv/reviews.csv')