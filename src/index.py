# TODO: Package all dependencies: cryptography, 
import pandas as pd
import urllib.parse
import boto3
import logging
import sqlalchemy
import os
from extractcsv import read_csvfile_into_dataframe
#from test_table import create_sales_tables
from load import *
#from transform_3nf import *
#from suppress_pii import encrypt_pii,decrypt_pii
from transform_3nf import third_normal_form
import suppress_pii as pii

from create_db import *

TRANSACTIONS_TABLE = "transactions"
PRODUCTS_TABLE = "products"
BASKET_ITEMS_TABLE = "basket_items"

s3 = boto3.client('s3')

# s3_r = boto3.resource('s3')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
def handler(event, context):
    LOGGER.info(f'Event structure: {event}')
    #Get the object from the event 
    #for i in event['Records']['s3']['bucket']['name']:
    #print(event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(f"KEY = {key}")
    file_name =  '/tmp/' + key.split("/")[-1]
    s3 = boto3.client('s3')
    s3.download_file(bucket, key, file_name)
    #lst = os.listdir()
    #print(lst)
    
    #Extract the csv
    data_df = read_csvfile_into_dataframe(file_name)
    #print(df)
    #Drop the card number column from the dataframe
    
    
    pii.drop_column(data_df, "card_number")
    # secretkey = pii.load_key()
    # pii.encrypt_pii(data_df, "customer_name")
    # 3. Generate UUID for transactions, then efficently represent the cleaned data
    table_dict = third_normal_form(data_df)     # TODO: In line 92, add UUID
    # print(table_dict[TRANSACTIONS_TABLE])
    # print(table_dict[PRODUCTS_TABLE])
    # print(table_dict[BASKET_ITEMS_TABLE])
    
    # SQS
    
    #LOAD
    
    # Connect to Redshift using Psycopg2
    (conn, cursor) = connect_db()
    create_tables(conn, cursor)
    load_mvp_tables(conn, cursor, table_dict)
    save_and_close_connection(conn, cursor)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    # try:
    #     response = s3.get_object(Bucket=bucket, Key=key)
    #     print(response)
    #      #rows=['Date,Store,Name,Product,PaymentType,Price,Transaction_Id']
    #     rows=[]
    #     for row in csv.DictReader(codecs.getreader('utf-8')(response['Body'])):
    # 	    rows.append(row)
    	   
    #     df = pd.DataFrame(rows)
    #     print(df)
    #     # body = response['Body']
    #     # csv_string = body.read().decode('utf-8')
    #     # file_name = StringIO(csv_string)
    #     # df = pd.read_csv(StringIO(csv_string))
    #     print(key)
    #     #df = read_csvfile_into_dataframe(file_name)
    #     #print("CONTENT TYPE: " + response['ContentType'])
    #     #return df
    # except Exception as e:
    #     print(e)
    #     print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
    #     raise e
   
    # my_bucket=s3_r.Bucket(bucket_name)
  
    
    # #To read the files loaded to S3 on a certain day. 
    # #The trigger should extract the files as soon as they are uploaded to S3 at 8 pm everyday.
    
    # dt=datetime.datetime.now().strftime("%d")
   
    # # print("2022/6/"+dt+'/')
    # filenames=[]
    # for object_summary in my_bucket.objects.filter(Prefix="2022/6/"+dt+'/'):
    #         filenames.append(object_summary.key)
    # print(filenames)        

    # for file in filenames:
    #     try:
    #         response = s3.get_object(Bucket=bucket_name, Key=file)
            
    #         #rows=['Date,Store,Name,Product,PaymentType,Price,Transaction_Id']
    #         rows=[]
    #         for row in csv.DictReader(codecs.getreader('utf-8')(response['Body'])):
    # 	        rows.append(row)
    	   
    #         df = pd.DataFrame(rows)
    #         print(df)
    #     except Exception as e:
    #         print(e)
    #         print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket_name))
    #         raise e
        