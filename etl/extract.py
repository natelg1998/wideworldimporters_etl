import pandas as pd
import os
from db_config.db_server import HOSTNAME
from sqlalchemy import create_engine

def create_connection():
    connection_url = f"mssql+pymssql://{HOSTNAME}/WideWorldImporters"
    eng = create_engine(connection_url)
    return eng

def extract_data(dataset):
    """

    :param dataset: specify the dataset you would like to extract from
    :return:
    """
    df = pd.read_sql(f"""
    SELECT *
    FROM {dataset}
    """, con=create_connection())

    return df


