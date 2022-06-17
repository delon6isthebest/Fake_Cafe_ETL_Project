from create_db import connect_to_db
from extractcsv import read_csvfile_into_dataframe
from load import load_table
import ticket_10 as pii 
from transform_3nf import *

def etl(db,test.csv):
        # connecting (Line 10 - line 23)
        connect_to_db()
        #Extract the csv
        df=read_csvfile_into_dataframe(test.csv)
        
        # Transform the csv PPI
        # Function to hide the card numbers of customers. Line 30: 
        pii.drop_column(df, 'card.number')
        # Function to load the key to open encryption. Line 67: 
        pii.load_key()
        
        #transform 3nf
        # print(df)
        df = split_basket_items(df)
        # print(df[["transaction_id", BASKET_COLUMN]])
        extract_product_details(df)
        # print(df)
        # print("\n\n")

        products_df = create_products_df(df)
        print(products_df)
        print()
        # print(products_df.index[(products_df['name'] == 'Latte') & (products_df['size'] == 'Large')])
        transactions_df = create_transactions_df(df, products_df)
        print(transactions_df)

        #Load the csv to the db
        #load product table in order to query
        load_table(products_df,'productsmvp')
        # TO DO individual tasks structure code in logical format so app.py can run
        # TO DO load transcations mvp
        

        
        
        


