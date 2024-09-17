import pytest
from lib.Utils import get_spark_session
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders
from lib.ConfigReader import get_app_config

def test_read_customers_df():
    spark = get_spark_session("LOCAL")
    customers_count = read_customers(spark,"LOCAL").count()
    print(f"Customers count: {customers_count}")
    assert customers_count==12435

def test_read_orders_df():
    spark = get_spark_session("LOCAL")
    orders_count = read_orders(spark,"LOCAL").count()
    print(f"Orders count: {orders_count}")
    assert orders_count==68884

def test_filter_closed_orders():
    spark = get_spark_session("LOCAL")
    orders_df = read_orders(spark,"LOCAL")
    filtered_orders_df = filter_closed_orders(orders_df)
    filtered_count = filtered_orders_df.count()
    print(f"Filtered Orders count: {filtered_count}")
    assert filtered_count == 7556

def test_read_config ():
    config = get_app_config("LOCAL")
    assert config['orders.file.path'] == "data/orders.csv"
