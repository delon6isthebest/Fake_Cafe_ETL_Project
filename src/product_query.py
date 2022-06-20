# Situation product details from the dataframe has been loaded into the table

# Import libraries
from extractcsv import *
from create_db import *
# from transform_3nf import *
import pandas as pd

# Query database with unique combinations of name, size, flavour and price
# Connection and cursor objects already exist
# Access the values in DF given the row index
def query_id(conn, cursor, row_index: int, products_df:pd.DataFrame):
    
    products_name = products_df['name'].loc[row_index]
    products_size = products_df['size'].loc[row_index]
    products_flavour = products_df['flavour'].loc[row_index]

    cursor.execute(f'SELECT id FROM productsmvp WHERE name = {products_name} AND size = {products_size} AND flavour = {products_flavour}')
    sql_id = cursor.fetchone()

    return sql_id
