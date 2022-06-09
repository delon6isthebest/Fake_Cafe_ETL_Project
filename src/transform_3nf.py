import pandas as pd
import numpy as np
import csv
TEST_CSV = "test.csv" # Located in 'src' folder
BASKET_COLUMN = "basket_items"

# TODO: Transform data_frame to 1st Normal Form
# First we will split the basket items into separate rows after each comma
def split_basket_items(df:pd.DataFrame) -> pd.DataFrame:
    df["transaction_id"] = df.index
    df[BASKET_COLUMN] = df[BASKET_COLUMN].apply(lambda x: x.split(","))
    return df.explode(BASKET_COLUMN, ignore_index=True)

# Split the product column into separate columns for each detail: size, name, flavour, price
def extract_product_details(df:pd.DataFrame) -> pd.DataFrame:
    # Extract the details of product: size, name, flavour, price
    def extract_details(product:str) -> list:
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
        return details
    
    df[BASKET_COLUMN] = df[BASKET_COLUMN].apply(extract_details)
    df[['size','name','flavour','price']] = pd.DataFrame(df[BASKET_COLUMN].to_list())
    del df[BASKET_COLUMN]
    return df


# TODO: Transform data_frame to 2nd Normal Form
# Split into multiple tables and assign a foreign key

# Extract the unique products into another DataFrame
def create_products_df(df):
    products_df = df[['name','size','flavour','price']]
    return products_df.drop_duplicates()

# Normalise the transactions: Replace product details with product_id
def create_transactions_df(df, products_df):
    df["product_id"] = df['name'] + '-' + df['size'] + '-' + df['flavour'] + '-' + df['price']

    def get_id(product_detail, products_df):
        return products_df.index[products_df['detail'] == product_detail].tolist()[0]

    products_df['detail'] = products_df['name'] + '-' + products_df['size'] + '-' + products_df['flavour'] + '-' + products_df['price']
    df["product_id"] = df["product_id"].apply(lambda x: get_id(x, products_df))
    for column in ['name', 'size', 'flavour', 'price']:
        del df[column]
    del products_df['detail']
    return df


# TODO: Transform data_frame to 3rd Normal Form


def read_csvfile_into_list(file_name: str):
    try:
        list_dicts = []
        with open(file_name, "r") as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                list_dicts.append(row)
    except FileNotFoundError:
        list_dicts = []
    return list_dicts

if __name__ == "__main__":
    df = pd.read_csv(TEST_CSV)
    del df['card_number']
    # print(df)
    df = split_basket_items(df)
    # print(df[["transaction_id", BASKET_COLUMN]])
    extract_product_details(df)
    # print(df)
    # print("\n\n")

    products_df = create_products_df(df)
    print(products_df)
    print()
    # print(products_df.index[(products_df['name'] == 'Latte') & (products_df['size'] == 'Large')])
    transactions_df = create_transactions_df(df, products_df)
    print(transactions_df)
    