import csv
from datetime import datetime
from dotenv import load_dotenv
import os
import pprint
import psycopg2



def connect_to_db():
    load_dotenv()
    db_host = os.environ.get("postgresql_host")
    db_user = os.environ.get("postgresql_user")
    db_password = os.environ.get("postgresql_pass")
    db = os.environ.get("postgresql_db")

    return psycopg2.connect(
        host = db_host,
        user = db_user,
        password = db_password,
        dbname = db
    )
    

def create_tables():
    conn = connect_to_db()
    cursor = conn.cursor()
    create_products_table = \
        """
        CREATE TABLE IF NOT EXISTS products(
            id SERIAL,
            prod_name varchar(200),
            prod_size varchar(10),
            prod_price decimal(19,2),
            PRIMARY KEY(id)
        );
        """
    create_customers_table = \
        """
        CREATE TABLE IF NOT EXISTS customers(
            id SERIAL,
            customer_name varchar(200),
            card_number varchar(20),
            PRIMARY KEY(id)
        );
        """    
    create_store_table = \
        """
        CREATE TABLE IF NOT EXISTS store(
            id SERIAL,
            location varchar(20),
            PRIMARY KEY(id)
        );
        """ 
    create_transaction_table = \
        """
        CREATE TABLE IF NOT EXISTS transactions(
            id SERIAL,
            timestamp TIMESTAMP,
            store_id int NOT NULL,
            customer_id int NOT NULL,
            product_id int NOT NULL,
            quantity int,
            cash_card varchar(10),
            PRIMARY KEY(id),
            FOREIGN KEY(store_id) references "store" (id),
            FOREIGN KEY(customer_id) references "customers" (id),
            FOREIGN KEY(product_id) references "products" (id)
        );
        """       
    cursor.execute(create_products_table)
    cursor.execute(create_customers_table)
    cursor.execute(create_store_table)
    cursor.execute(create_transaction_table)
    conn.commit()
    cursor.close()
    conn.close()

create_tables()