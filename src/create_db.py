# Thought: How often should we connect and then close the connection?
# from datetime import datetime
from dotenv import load_dotenv # To load credentials
import os
# import pprint
import psycopg2 # To connect to PostgreSQL Server


# Thought: Could error handle connection, if we fail to connect
def connect_to_db():
    load_dotenv()
    db_host = os.environ.get("postgresql_host")
    db_user = os.environ.get("postgresql_user")
    db_password = os.environ.get("postgresql_pass")
    db = os.environ.get("postgresql_db")

    conn = psycopg2.connect(
        host = db_host,
        user = db_user,
        password = db_password,
        dbname = db
    )
    return (conn, conn.cursor())

# Save changes to DB and then close cursor + connection
def save_and_close_connection(conn, cursor):
    conn.commit()
    cursor.close()
    conn.close()

# Create tables if they don't exist: products, customers, stores, transactions
def create_tables(conn, cursor):
    create_products_table = \
        """
        CREATE TABLE IF NOT EXISTS products(
            id SERIAL PRIMARY KEY,
            prod_name VARCHAR(200),
            prod_size VARCHAR(10),
            prod_price DECIMAL(19,2)
        );
        """
    create_customers_table = \
        """
        CREATE TABLE IF NOT EXISTS customers(
            id SERIAL PRIMARY KEY,
            customer_name VARCHAR(200),
            card_number VARCHAR(20)
        );
        """    
    create_store_table = \
        """
        CREATE TABLE IF NOT EXISTS stores(
            id SERIAL PRIMARY KEY,
            location VARCHAR(20)
        );
        """ 
    # Thought: Should we also add "bill DECIMAL(19,2)" to the transactions table?
    create_transaction_table = \
        """
        CREATE TABLE IF NOT EXISTS transactions(
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            store_id int NOT NULL REFERENCES stores (id),
            customer_id int NOT NULL REFERENCES customers (id),
            product_id int NOT NULL REFERENCES products (id),
            quantity int,
            cash_card VARCHAR(10)
        );
        """       
    cursor.execute(create_products_table)
    cursor.execute(create_customers_table)
    cursor.execute(create_store_table)
    cursor.execute(create_transaction_table)
    conn.commit()

# Prevents calling those methods when importing to another module
if __name__ ==  "__main__":
    (conn, cursor) = connect_to_db()
    create_tables(conn, cursor)
    save_and_close_connection(conn, cursor)

