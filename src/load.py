from json import load
import psycopg2
import os
import pandas as pd
import psycopg2.extras as extras
from sqlalchemy import create_engine
from dotenv import load_dotenv
from product_query import query_product_ids, replace_index_with_queried_id

TRANSACTIONS_TABLE = "transactions"
PRODUCTS_TABLE = "products"
BASKET_ITEMS_TABLE = "basket_items"
from create_conn_string import create_conn_string

def load_table(df,table):  #pass in pandasdataframe and table in database
    conn_string = create_conn_string()
    db = create_engine(conn_string)
    conn = db.connect()
    df.to_sql(table, con=conn, if_exists='append',
            index=False)

    conn.close()


def load_mvp_tables(conn, cursor, table_dict):
    """
    Load the three tables into database: transactions, products and then basket_items 
    Each item of table_dict has the form (table_name, table_df)
    """
    transactions_df = table_dict[TRANSACTIONS_TABLE]
    load_table(transactions_df, TRANSACTIONS_TABLE)
    products_df = table_dict[PRODUCTS_TABLE]
    baskets_df = table_dict[BASKET_ITEMS_TABLE]
    new_products_df = products_df                       #TODO: Call the function defined in product_query.py
    load_table(new_products_df, PRODUCTS_TABLE)
    query_product_ids(conn, cursor, products_df)
    replace_index_with_queried_id(baskets_df, products_df)
    load_table(baskets_df, BASKET_ITEMS_TABLE)
