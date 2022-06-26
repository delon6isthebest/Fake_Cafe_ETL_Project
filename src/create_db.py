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
        DROP TABLE IF EXISTS "products";
        DROP SEQUENCE IF EXISTS products_id_seq;
        CREATE SEQUENCE products_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;

        CREATE TABLE "public"."products" (
            "id" integer DEFAULT nextval('products_id_seq') NOT NULL,
            "name" character varying,
            "size" character varying,
            "flavour" character varying,
            CONSTRAINT "products_pkey" PRIMARY KEY ("id")
        ) WITH (oids = false);
        """ 
    # Create the transactions table
    create_transaction_table = \
        """
        DROP TABLE IF EXISTS "transactions";
        DROP SEQUENCE IF EXISTS "transactions_ID_seq";
        CREATE SEQUENCE "transactions_ID_seq" INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;

        CREATE TABLE "public"."transactions" (
            "ID" integer DEFAULT nextval('"transactions_ID_seq"') NOT NULL,
            "transaction_id" uuid NOT NULL,
            "timestamp" timestamp NOT NULL,
            "store" character varying,
            "customer_name" character varying,
            "total_price" numeric,
            "cash_or_card" character varying,
            "load_date" date DEFAULT CURRENT_DATE NOT NULL,
            CONSTRAINT "transactions_pkey" PRIMARY KEY ("ID"),
            CONSTRAINT "transactions_transaction_id" UNIQUE ("transaction_id")
        ) WITH (oids = false);
        """      
    create_basket_items_table=\
        """
        DROP TABLE IF EXISTS "basket_items";
            DROP SEQUENCE IF EXISTS "basket_items_ID_seq";
            CREATE SEQUENCE "basket_items_ID_seq" INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;

            CREATE TABLE "public"."basket_items" (
                "ID" integer DEFAULT nextval('"basket_items_ID_seq"') NOT NULL,
                "transaction_id" uuid NOT NULL,
                "product_id" integer NOT NULL,
                "price" numeric,
                "quantity" integer,
                "load_date" date DEFAULT CURRENT_DATE NOT NULL,
                CONSTRAINT "basket_items_pkey" PRIMARY KEY ("ID")
            ) WITH (oids = false);
        ALTER TABLE ONLY "public"."basket_items" ADD CONSTRAINT "basket_items_product_id_fkey" FOREIGN KEY (product_id) REFERENCES products(id) NOT DEFERRABLE;
        ALTER TABLE ONLY "public"."basket_items" ADD CONSTRAINT "basket_items_transaction_id_fkey" FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id) NOT DEFERRABLE;
        """
    create_etl_last_run_table=\
        """
        DROP TABLE IF EXISTS "etl_last_run";
        CREATE TABLE "public"."etl_last_run" (
            "last_run_time" timestamp NOT NULL
        ) WITH (oids = false);
        """
    cursor.execute(create_products_table)
    cursor.execute(create_transaction_table)
    cursor.execute(create_basket_items_table)
    cursor.execute(create_etl_last_run_table)
    conn.commit()

# Prevents calling those methods when importing to another module
if __name__ ==  "__main__":
    (conn, cursor) = connect_to_db()
    create_tables(conn, cursor)
    save_and_close_connection(conn, cursor)

