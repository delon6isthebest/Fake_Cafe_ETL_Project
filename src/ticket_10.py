import hashlib
import pandas as pd

def hash_func(customer_info):
    str_hash=customer_info.encode()                 # Encode the customer info using UTF-8 default
    return hashlib.sha256(str_hash).hexdigest()     # Hash using SHA-256

def create_hash_feature(df,existing_feat,customer_info):                    # Apply hashing function to the feature
    if pd.dtypes(df['existing_feat'])!=str:
        df['existing_feat']=df['existing_feat'].astype(str)
    df[customer_info+'_hash'] = df[existing_feat].apply(lambda x:hashlib.sha256(x.encode()).hexdigest())
    df.drop(columns='existing_feat',inplace=True)                           # Drop the original feature
    return  df