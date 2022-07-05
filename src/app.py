from heapq import merge
from json import load
from create_db import connect_to_db
from extractcsv import read_csvfile_into_dataframe
from load import load_table
import suppress_pii as pii 
from transform_3nf import *
from datetime import date
from datetime import datetime
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

def etl(filename):
        
        (conn, cursor)=connect_to_db()
        
        last_run=get_last_run_time(conn, cursor) # get the last run time from database
        #print('last run',str(last_run[0])[:10])
        if last_run is None or str(last_run[0])[:10] != str(date.today()) : # perform etl only if last run date is not equal to today's date
                
                #Extract the csv
                df=read_csvfile_into_dataframe(filename)
                
                # Drop the card number column
                # Function to hide the card numbers of customers. Line 30: 
                df=pii.drop_column(df, 'card_number')   #Drop the card number column from the dataframe
                rt=transform_3nf(df)
                #print(rt['products'])
                rt['products']=get_new_products(rt['products']) # get only those products which are not available in the products table. Insert on the incremental products
                #print(rt['products'])
                if not rt['products'].empty:
    
                        load_table(rt['products'],'products')
                rt['basket_items']=replace_index_with_queried_id(rt['basket_items'])
                rt['basket_items']=rt['basket_items'].drop(['timestamp',
        'store', 'customer_name', 'total_price', 'cash_or_card','basket_items'],axis=1) # drop columns from basket which are not required in for insertion into the database
                rt['transactions']=pii.encrypt_pii(rt['transactions'],'customer_name')  #Encrypt the customer name.
                
                
                #print('PRODUCTS DATAFRAME')
                #print(rt['products'])
                #print('TRANSACTIONS DATAFRAME')
                #print(rt['transactions'].head())
                #print('BASKET DATAFRAME')
                #print(rt['basket_items'].head(20))
                #print(rt['basket_items'].columns)      
                
                # #Load the csv to the db
                today = date.today() # get today's date
                df_date=pd.DataFrame(pd.to_datetime(rt['transactions']['timestamp']).dt.date)
                df_date['timestamp']=df_date['timestamp'].astype(str)
                #print(df_date['timestamp'].unique())
        
                if str(today) in df_date['timestamp'].unique(): # load only if today's date is available in the df's timestamp column. This is to ensure the a file is loaded only if it contains today's data
                        load_table(rt['transactions'],'transactions')               
                        load_table(rt['basket_items'],'basket_items')
                # # TO DO individual tasks structure code in logical format so app.py can run
                # # TO DO load transcations
                
                #Insert last run date to etl_last_run table
                insert_query="INSERT INTO etl_last_run VALUES ("+ "'" + str(datetime.now()) + "'" + ")"
                cursor.execute(insert_query)
                conn.commit()
# Filters out those products from the df which are already available in the database
def get_new_products(df):                       
        (conn, cursor)=connect_to_db()
        products_query='SELECT * FROM products'
        cursor.execute(products_query)
        pdt_table_data=cursor.fetchall()
        pdt_table_data_df=pd.DataFrame.from_records(pdt_table_data, columns=[x[0] for x in cursor.description])
        pdt_table_data_df['master'] = 'master'
        pdt_table_data_df.set_index('master', append=True, inplace=True)
        #print(pdt_table_data_df)
        df['daily'] = 'daily'
        df.set_index('daily', append=True, inplace=True)
        #print(df)
        
        merged = pdt_table_data_df.append(df)
        #print(merged)
        merged = merged.drop_duplicates(subset=['name', 'size','flavour'], keep=False).sort_index()
        #print('Merged:',merged)
        idx = pd.IndexSlice
        #print(idx)
        if not merged.empty:
                merged=merged.loc[idx[:, 'daily'], :]
                #print(merged)
                merged=merged.reset_index(drop=True)
                #print(merged)
        df=merged.drop(['id'],axis=1)
        #print(df)
        return df
        
        # Gets etl's last run date
def get_last_run_time(conn, cursor):
        query="SELECT * FROM etl_last_run" # last run time is stored in the table
        cursor.execute(query)
        last_run_time=cursor.fetchone()
        return last_run_time
        

if __name__=='__main__':
    etl('test.csv')  
