from etl.transform import transform_data, transform_factorder
import os

extracts_dir = os.path.realpath('./table_extracts')

table_names = {
    'Application.Countries': 'Dim_Countries',
    'Application.Cities': 'Dim_Cities',
    'Application.StateProvinces': 'Dim_States',
    'Sales.Customers': 'Dim_Customers',
    'Sales.Orders': 'Orders',
    'Sales.OrderLines': 'OrderDetails'
}

# This will get most of the extracted data. Will have to call
for t in table_names:
    df = transform_data(t)
    df.to_parquet(path=os.path.join(extracts_dir, f"{table_names[t]}.parquet"))

fact_order = transform_factorder()
fact_order.to_parquet(path=os.path.join(extracts_dir, "Fact_Order.parquet"))
