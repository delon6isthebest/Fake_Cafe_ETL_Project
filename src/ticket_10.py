from cmath import nan
import pandas as pd
from cryptography.fernet import Fernet

df = pd.read_csv('test.csv')          #Reading csv file from src directory

def generate_key():                        #Generates a key and save it into a file
    key = Fernet.generate_key()
    with open("secretkey.txt", "wb") as key_file:
        key_file.write(key)

def load_key():                                    #Loads the key named `secret.key` from the current directory.
    return open("secretkey.txt", "rb").read()

def encryption(value):  
    key = load_key()        #Accessing previously generated key
    fernet = Fernet(key)        #Instance the fernet class with the key          
    encrypted_value = fernet.encrypt(value.encode())   #use the instance to encrypt str, str as to be encoded to bytes str
    return encrypted_value                            #Return encrypted str

def encrypt_pii(df,column):   
    df[column]=df[column].apply(lambda x:encryption(x)) #enrypting the column
    return df            #return the df with encrypted column

print(encrypt_pii(df,'customer_name'))                   #prints dataframe with the pii column decrypted

load_key()     #loads key for utilisation

def decrypt_message(encrypted_value):             #To decrypt an encrypted message
    key = load_key()
    fernet = Fernet(key)                        # then use the Fernet class instance and use key 
    decrypted_message = fernet.decrypt(encrypted_value)
    decrypted_msg = decrypted_message.decode()              #Decodes the previously encrypted values
    return decrypted_msg

def decrypt_pii(df,column):
    df[column]=df[column].apply(lambda x:decrypt_message(x)) #decrypts the previously encrypted column
    print(df[column])  #prints decrypted column
    return df                  #return the df with decrypted column
print(decrypt_pii(df,'customer_name'))               #prints dataframe with the pii column decrypted