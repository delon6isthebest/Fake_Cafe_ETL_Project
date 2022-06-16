from sqlalchemy import create_engine
from create_conn_string import create_conn_string

def load_table(df,table):  #pass in pandasdataframe and table in database
    conn_string = create_conn_string()
    db = create_engine(conn_string)
    conn = db.connect()
    df.to_sql(table, con=conn, if_exists='append',
            index=False)

    conn.close()