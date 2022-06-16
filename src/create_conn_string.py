from dotenv import load_dotenv # To load credentials
import os

def create_conn_string():
    load_dotenv()
    postgresql_host = os.environ.get("postgresql_host")
    postgresql_user = os.environ.get("postgresql_user")
    postgresql_pass = os.environ.get("postgresql_pass")
    postgresql_db = os.environ.get("postgresql_db")

    # Creating connection string
    conn_string = f'postgresql://{postgresql_user}:{postgresql_pass}@{postgresql_host}/{postgresql_db}'

    return conn_string




    