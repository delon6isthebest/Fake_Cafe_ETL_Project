# Situation product details from the dataframe has been loaded into the table

# Import libraries
from transform_3nf import * # Only needed when testing out how they combine
from load import load_table
from create_db import *
import pandas as pd

# Query database with unique combinations of name, size, flavour and price
# Connection and cursor objects already exist
# Access the values in DF given the row index
def query_id(conn, cursor, row_index: int, df:pd.DataFrame):
    values = df[['name', 'size', 'flavour']].loc[row_index].tolist()
    print(values)
    select_query =\
    """
    SELECT id
    FROM productsmvp
    WHERE name = %s and size = %s and flavour = %s 
    ;
    """
    # Note values is a list of values to be inserted into the SQL command at the placeholders
    try:
        cursor.execute(select_query, values) 
        conn.commit()
        row = cursor.fetchone()
        print(row)
        return row[0]
    except Exception as e:
        print(e)    # Product with given details does not exist
        return row_index * 2

# Store queried products id's in a separate column
def query_product_ids(conn, cursor, df:pd.DataFrame):
    df['queried_id'] = df.index
    df['queried_id'] = df['queried_id'].apply(lambda x : query_id(conn, cursor, x, df))

# After querying ids, replace them in the basket_table
def replace_index_with_queried_id(df, products_df):
    df['product_id'] = df['product_id'].apply(lambda x: products_df['queried_id'].loc[x])
    return df



if __name__ == "__main__":
    # Connect to db and store in variable by creating the connection object and the cursor object
    (conn, cursor) = connect_to_db()
    df = pd.read_csv(TEST_CSV)
    del df['card_number']
    df = split_basket_items(df)
    extract_product_details(df)
    products_df = create_products_df(df)
    load_table(products_df)
    transactions_df = create_transactions_df(df, products_df)
    query_product_ids(conn, cursor, products_df)
    print(products_df)
    print()
    print(transactions_df)
    print()
    replace_index_with_queried_id(transactions_df, products_df)
    print(transactions_df)
