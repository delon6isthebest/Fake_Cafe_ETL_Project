# Situation product details from the dataframe has been loaded into the table

# Import libraries
from transform_3nf import *
from create_db import *
import pandas as pd

# Query database with unique combinations of name, size, flavour and price
# Connection and cursor objects already exist
# Access the values in DF given the row index
def query_id(conn, cursor, row_index: int, df:pd.DataFrame):
    values = df[['name', 'size', 'flavour']].iloc[row_index]


    cursor.execute(f'SELECT * FROM products')
    rows = cursor.fetchall()
    select_query =\
    """
    SELECT name, size, flavour
    FROM products
    WHERE name = %s, size = %s, flavour = %s 
    ;
    """
    # Note values is a list of values to be inserted into the SQL command at the placeholders
    cursor.execute(select_query, values) 
    row = cursor.fetchone()
    return row[0]

# Store queried products id's in a separate column
def query_product_ids(conn, cursor, df:pd.DataFrame):
    df['queried_id'] = (df.index).apply(lambda x : query_id(conn, cursor, x, df))

# After querying ids, replace them in the basket_table
def replace_index_with_queried_id(df, products_df):
    df['product_id'] = df['product_id'].apply(lambda x: products_df['queried_id'].iloc[x])
    return df



if __name__ == "__main__":
    # Connect to db and store in variable by creating the connection object and the cursor object
    (conn, cursor) = connect_to_db()
    


