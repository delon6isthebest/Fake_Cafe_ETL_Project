import pandas as pd
import numpy as np
import csv
from load import *
TEST_CSV = "test.csv" # Located in 'src' folder
BASKET_COLUMN = "basket_items"
PRODUCT_COLUMNS = ["name", "size", "flavour"]
TRANSACTION_COLUMNS = ["timestamp", "store", "customer_name", "total_price", "cash_or_card"]
BASKET_ITEMS_COLUMNS = ["transaction_id", "product_id", "price", "quantity"]
TRANSACTION_ID = "transaction_id"
PRODUCT_ID = "product_id"
TRANSACTIONS_TABLE = "transactions"
PRODUCTS_TABLE = "products"
BASKET_ITEMS_TABLE = "basket_items"

# TODO: Transform data_frame to 1st Normal Form
# First we will split the basket items into separate rows after each comma (counting quantity per basket)
def split_basket_items(df:pd.DataFrame) -> pd.DataFrame:
    df[TRANSACTION_ID] = df.index
    def count_items(basket: str) -> list:
        item_count_dict = {}
        items_list = [item.strip() for item in basket.split(",")]
        for item in items_list:
            item_count_dict[item] = item_count_dict.get(item, 0) + 1
        return list(item_count_dict.items())
    df[BASKET_COLUMN] = df[BASKET_COLUMN].apply(lambda x: count_items(x))
    return df.explode(BASKET_COLUMN, ignore_index=True)

# Split the product column into separate columns for each detail: size, name, flavour, price
def extract_product_details(df:pd.DataFrame) -> pd.DataFrame:
    # Extract the details of product: size, name, flavour, price
    def extract_details(product_count:tuple) -> list:
        product = product_count[0]
        size_plus_other_details = product.strip().split(" ", 1)
        size = size_plus_other_details[0]
        other_details = [detail.strip() for detail in size_plus_other_details[1].split("-")]
        if len(other_details) == 2:
            price = other_details[-1]
            other_details[-1] = "Original"
            other_details.append(price)
        name = other_details[0]
        if name[:9] == "Flavoured":
            other_details[0] = name[9:].lstrip().capitalize()
        details = [size]
        details.extend(other_details)
        details.append(product_count[1])
        return details
    
    df[BASKET_COLUMN] = df[BASKET_COLUMN].apply(extract_details)
    df[['size','name','flavour','price','quantity']] = pd.DataFrame(df[BASKET_COLUMN].to_list())
    del df[BASKET_COLUMN]
    return df


# Extract some columns for a subtable: 'transactions' and 'products', and replace with row_index
def extract_subtable(df:pd.DataFrame, subcolumns, foreign_key:str):
    subtable_df = df[subcolumns].drop_duplicates()
    subtable_df['combined'] = subtable_df.astype(str).apply(np.sum, axis=1)
    
    def get_index(combined_value, subtable_df):
        try:
            return subtable_df.index[subtable_df['combined'] == combined_value].tolist()[0]
        except:
            return None

    df[foreign_key] = df[subcolumns].astype(str).apply(np.sum, axis=1).apply(lambda x: get_index(x, subtable_df))
    for column in subcolumns:
        del df[column]
    del subtable_df['combined']
    return subtable_df


# Normalise the transactions: Replace product details with product_id

# TODO: Transform data_frame to 3rd Normal Form

def transform_3nf(data_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    transactions_df = extract_subtable(data_df, TRANSACTION_COLUMNS, TRANSACTION_ID)
    # customers_df = extract_subtable(transactions_df, ['customer_name'], 'customer_id')
    # print(customers_df)
    # print()
    # stores_df = extract_subtable(transactions_df, ['store'], 'store_id')
    # print(stores_df)
    # print()
    # print(transactions_df)
    # print()
    basket_df = split_basket_items(data_df)
    extract_product_details(basket_df)
    products_df = extract_subtable(basket_df, PRODUCT_COLUMNS, PRODUCT_ID)
    # print(products_df)
    # print()
    # print(basket_df)
    # print()
    return {
        PRODUCTS_TABLE: products_df,
        TRANSACTIONS_TABLE: transactions_df,
        BASKET_ITEMS_TABLE: basket_df[BASKET_ITEMS_COLUMNS]
    }

if __name__ == "__main__":
    
    data_df = pd.read_csv(TEST_CSV)
    del data_df['card_number']
    transform_3nf(data_df)
    
    # Print basket items which are multiple-buys within a basket
    # print(basket_df.loc[basket_df.index[basket_df['quantity'] != 1].tolist()])

  
