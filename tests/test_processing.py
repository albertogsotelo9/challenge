import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from data_processing import process_data




@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Testapp").getOrCreate()

def test_valid_inputs(spark):
    data = [
        Row(user_id=1, user_name="Alice", email="alice@example.com", purchase_datetime="2023-06-01 10:00:00", purchase_amount=100.0),
        Row(user_id=1, user_name="Alice", email="alice@example.com", purchase_datetime="2023-06-15 10:00:00", purchase_amount=200.0),
        ]
    df = spark.createDataFrame(data)
    result = process_data(df)
    
    assert result.count() == 1
    assert result.collect()[0]["total_purchase_amount"] == 300.0
    
def test_null_values_filtered(spark):
    data = [
        Row(user_id=None, user_name="Bob", email=None, purchase_datetime="2023-01-01 09:00:00", purchase_amount=50.0),
    ]    

    df = spark.createDataFrame(data)
    result = process_data(df)
    
    assert result.count() == 0
    
    
def test_out_of_range_filtered(spark):
    data = [
        Row(user_id=2, user_name="Carol", email="carol@example.com", purchase_datetime="2022-12-31 23:59:59", purchase_amount=150.0),
    ]
    
    df = spark.createDataFrame(data)
    result = process_data(df)
    assert result.count() == 0
    
def test_empty_input(spark):
    df = spark.createDataFrame([], schema= "user_id INT, user_name STRING, email STRING, purchase_datetime STRING, purchase_amount FLOAT")    
    result = process_data(df)
    assert result.count() == 0
