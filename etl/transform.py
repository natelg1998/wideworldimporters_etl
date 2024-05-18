import pandas as pd
from etl.extract import extract_data
import numpy as np
import os


def transform_data(dataset):
    if dataset == 'Application.Countries':
        return transform_countries(dataset)
    elif dataset == 'Application.StateProvinces':
        return transform_states(dataset)
    elif dataset == 'Application.Cities':
        return transform_cities(dataset)
    elif dataset == 'Sales.Customers':
        return transform_customers(dataset)
    elif dataset == 'Sales.Orders':
        return transform_orders(dataset)
    elif dataset == 'Sales.OrderLines':
        return transform_orderline(dataset)
    else:
        return "That dataset does not have a defined ETL task"


def transform_countries(dataframe):
    df = extract_data(dataframe)
    df = df.loc[:, ['CountryID', 'CountryName', 'Continent', 'Region', 'Subregion']]
    df['CountryID'] = df['CountryID'].astype(np.int64)
    df['CountryName'] = df['CountryName'].astype('string')
    df['Continent'] = df['Continent'].astype('string')
    df['Region'] = df['Region'].astype('string')
    df['Subregion'] = df['Subregion'].astype('string')
    return df


def transform_states(dataframe):
    df = extract_data(dataframe)
    df = df.loc[:, ['StateProvinceID', 'CountryID', 'StateProvinceCode', 'StateProvinceName', 'SalesTerritory']]
    df['StateProvinceID'] = df['StateProvinceID'].astype(np.int64)
    df['CountryID'] = df['CountryID'].astype(np.int64)
    df['StateProvinceCode'] = df['StateProvinceCode'].astype('string')
    df['StateProvinceName'] = df['StateProvinceName'].astype('string')
    df['SalesTerritory'] = df['SalesTerritory'].astype('string')
    return df


def transform_cities(dataframe):
    df = extract_data(dataframe)
    df = df.loc[:, ['CityID', 'CityName', 'StateProvinceID']]
    df['CityID'] = df['CityID'].astype(np.int64)
    df['StateProvinceID'] = df['StateProvinceID'].astype(np.int64)
    df['CityName'] = df['CityName'].astype('string')
    return df


def transform_customers(dataframe):
    df = extract_data(dataframe)
    df = df.loc[:,
         ['CustomerID', 'CustomerName', 'CustomerCategoryID', 'DeliveryCityID', 'PostalCityID', 'CreditLimit']]
    df['CustomerID'] = df['CustomerID'].astype(np.int64)
    df['CustomerName'] = df['CustomerName'].astype('string')
    df['CustomerCategoryID'] = df['CustomerCategoryID'].astype(np.int64)
    df['DeliveryCityID'] = df['DeliveryCityID'].astype(np.int64)
    df['PostalCityID'] = df['PostalCityID'].astype(np.int64)
    df['CreditLimit'] = df['CreditLimit'].astype(np.float64)
    return df


def transform_orders(orders_dataframe):
    orders_df = extract_data(orders_dataframe)
    orders_df = orders_df.loc[:, ['OrderID', 'CustomerID', 'SalespersonPersonID', 'OrderDate', 'ExpectedDeliveryDate',
                                  'CustomerPurchaseOrderNumber']]
    orders_df['OrderID'] = orders_df['OrderID'].astype(np.int64)
    orders_df['CustomerID'] = orders_df['CustomerID'].astype(np.int64)
    orders_df['SalespersonPersonID'] = orders_df['SalespersonPersonID'].astype(np.int64)
    orders_df['OrderDate'] = pd.to_datetime(orders_df['OrderDate'])
    orders_df['OrderDate'] = orders_df['OrderDate'].dt.date
    orders_df['ExpectedDeliveryDate'] = pd.to_datetime(orders_df['ExpectedDeliveryDate'])
    orders_df['ExpectedDeliveryDate'] = orders_df['ExpectedDeliveryDate'].dt.date
    orders_df['CustomerPurchaseOrderNumber'] = orders_df['CustomerPurchaseOrderNumber'].astype(np.int64)
    return orders_df


def transform_orderline(order_details_dataframe):
    order_details_df = extract_data(order_details_dataframe)
    order_details_df = order_details_df.loc[:, ['OrderLineID','OrderID', 'StockItemID', 'Quantity', 'UnitPrice', 'TaxRate']]
    order_details_df['OrderLineID'] = order_details_df['OrderLineID'].astype(np.int64)
    order_details_df['OrderID'] = order_details_df['OrderID'].astype(np.int64)
    order_details_df['StockItemID'] = order_details_df['StockItemID'].astype(np.int64)
    order_details_df['Quantity'] = order_details_df['Quantity'].astype(np.int64)
    order_details_df['UnitPrice'] = order_details_df['UnitPrice'].astype(np.float64)
    order_details_df['Subtotal'] = round(order_details_df['Quantity'] * order_details_df['UnitPrice'], 2)
    order_details_df['TaxRate'] = order_details_df['TaxRate'].astype(np.float64)
    order_details_df['Total'] = round(order_details_df['Subtotal'] * (1 + order_details_df['TaxRate']), 2)
    return order_details_df


def transform_factorder():
    orders_df = pd.read_parquet(os.path.join(os.path.realpath('.'), 'table_extracts', 'Orders.parquet'))
    orderdetails_df = pd.read_parquet(os.path.join(os.path.realpath('.'), 'table_extracts', 'OrderDetails.parquet'))
    fact_order = orders_df.merge(orderdetails_df, on='OrderID')
    return fact_order
