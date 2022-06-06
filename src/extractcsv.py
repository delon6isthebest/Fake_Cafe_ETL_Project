
import csv
import pandas as pd

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

# print(read_csvfile_into_list("test.csv"))

def read_csvfile_into_dataframe(file_name: str):
    try:
        return pd.read_csv(file_name)
    except FileNotFoundError:
        return None

# print(read_csvfile_into_dataframe("test.csv"))