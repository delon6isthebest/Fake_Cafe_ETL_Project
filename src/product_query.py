# Situation product details from the dataframe has been loaded into the table

# Import libraries
from transform_3nf import *
from create_db import *
import pandas as pd

# Query database with unique combinations of name, size, flavour and price
# Connection and cursor objects already exist
# Access the values in DF given the row index
def query_id(conn, cursor, row_index: int, df:pd.DataFrame, table_name: str):
    [name, size, flavout, price] = df


    cursor.execute(f'SELECT * FROM products')
    rows = cursor.fetchall()
    """
    SELECT name, size, flavour, price
    );
    """



if __name__ == "__main__":
    # Connect to db and store in variable by creating the connection object and the cursor object
    (conn, cursor) = connect_to_db()
    


