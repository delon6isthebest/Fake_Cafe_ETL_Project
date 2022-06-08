import hashlib
from cryptography.fernet import Fernet
from faker import Faker
import pandas as pd
pd.set_option('display.max_colwidth', None)

def hash_func(customer_info):
    str_hash=customer_info.encode()                 # Encode the customer info using UTF-8 default
    return hashlib.sha256(str_hash).hexdigest()     # Hash using SHA-256

def create_hash_feature(df,existing_feat):
    print(df.dtypes[existing_feat])                                       # Apply hashing function to the feature
    if df.dtypes[existing_feat]!=str:
        df[existing_feat]=df[existing_feat].astype(str)
    df[existing_feat+'_hash'] = df[existing_feat].apply(lambda x:hash_func(x))
    df.drop(columns=existing_feat,inplace=True)                           # Drop the original feature
    return  df

# # Test code with test.csv
df=pd.read_csv('test.csv')                                          
#print(df.head())
#print(df['customer_name'].nunique())

#print(create_hash_feature(df,'customer_name'))

def drop_column(df,column):
    df.drop(columns=column,inplace=True)

drop_column(df,'card_number')

def suppress_pii(df,column):
    fake=Faker()
    df[column]=df[column].apply(lambda x:fake.name())
    return df

print(suppress_pii(df,'customer_name'))

def encryption(value):
    key = Fernet.generate_key()             # Instance the Fernet class with the key
    fernet = Fernet(key)                    # then use the Fernet class instance 
    encrypted_value = fernet.encrypt(value.encode())     #to encrypt the string string must be encoded to byte string before encryption
    return encrypted_value

def encrypt_pii(df,column):   
        df[column]=df[column].apply(lambda x:encryption(x))
        print(df[column])
        return df

print(encrypt_pii(df,'customer_name'))