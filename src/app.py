from create_db import connect_to_db
from extractcsv import read_csvfile_into_dataframe
from load import load_table
import ticket_10 as pii 
from transform_3nf import *
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', -1)

def etl(filename):
        # connecting (Line 10 - line 23)
        connect_to_db()
        #Extract the csv
        df=read_csvfile_into_dataframe(filename)
        
        # Transform the csv PII
        # Function to hide the card numbers of customers. Line 30: 
        df=pii.drop_column(df, 'card_number')   #Drop the card number column from the dataframe
        df=pii.encrypt_pii(df,'customer_name')  #Encrypt the customer name.
        
        #transform into the third normal form
        df = split_basket_items(df)
        # print(df[["transaction_id", BASKET_COLUMN]])
        df=extract_product_details(df)

        products_df = create_products_df(df)
        print(products_df.head())
        # print(products_df.index[(products_df['name'] == 'Latte') & (products_df['size'] == 'Large')])
        transactions_df = create_transactions_df(df, products_df)
        print(transactions_df.head())

        #Load the csv to the db
        #load product table in order to query
        load_table(products_df,'products')
        # TO DO individual tasks structure code in logical format so app.py can run
        # TO DO load transcations mvp
        load_table(transactions_df,'transactions')
        

if __name__=='__main__':
    etl('test.csv')  
