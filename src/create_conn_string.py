from dotenv import load_dotenv # To load credentials
from create_db import load_db_credentials
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import os

def create_url():  
    db_dict = load_db_credentials()
    url = URL.create(
        drivername='redshift+redshift_connector', # indicate redshift_connector driver and dialect will be used
        host=db_dict['endpoint'], # Amazon Redshift host
        port=db_dict['port'], # Amazon Redshift port
        database='dev_delon6_team1', # Amazon Redshift database
        username=db_dict['login'], # Amazon Redshift username
        password=db_dict['password'] # Amazon Redshift password
    )
    return url




    