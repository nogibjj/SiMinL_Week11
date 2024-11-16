# test_etl_pokemon.py

import pytest
from pyspark.sql import SparkSession
from mylib.ETL import extract, transform, load
from pyspark.sql.functions import col
import os
from dotenv import load_dotenv
import requests

# Load environment variables
# (make sure you have the .env file configured)
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")


# Initialize Spark session for testing
@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder.appName("ETL Pokemon Data Test").getOrCreate()
    yield spark_session
    spark_session.stop()


# Test data extraction
def test_extract(spark):
    url = "https://gist.githubusercontent.com/armgilles/194bcff35001e7eb53a2a8b441e8b2c6/raw/92200bc0a673d5ce2110aaad4544ed6c4010f687/pokemon.csv"
    df_spark = extract(spark, url)
    assert df_spark is not None, "Extraction failed, DataFrame is None"
    assert df_spark.count() > 0, "Extracted DataFrame is empty"
    print("Extract function passed.")


# Test data transformation
def test_transform(spark):
    url = "https://gist.githubusercontent.com/armgilles/194bcff35001e7eb53a2a8b441e8b2c6/raw/92200bc0a673d5ce2110aaad4544ed6c4010f687/pokemon.csv"
    df_spark = extract(spark, url)
    df_transformed = transform(df_spark)
    assert (
        "Type" in df_transformed.columns
    ), "'Type' column is missing after transformation"
    assert (
        df_transformed.select("Type").distinct().count() > 0
    ), "Type column has no distinct values"
    print("Transform function passed.")


# Test data loading into Databricks
def test_load(spark):
    table_name = "pokemon_data_test"
    url = "https://gist.githubusercontent.com/armgilles/194bcff35001e7eb53a2a8b441e8b2c6/raw/92200bc0a673d5ce2110aaad4544ed6c4010f687/pokemon.csv"
    df_spark = extract(spark, url)
    df_transformed = transform(df_spark)
    load(df_transformed, table_name)
    # Check if table exists in Databricks
    tables = spark.sql("SHOW TABLES").filter(col("tableName") == table_name)
    assert tables.count() == 1, f"Table {table_name} does not exist in Databricks"
    print("Load function passed.")


# Test SQL query execution
def test_spark_sql_query(spark):
    table_name = "pokemon_data_test"
    # Ensure the table exists
    test_load(spark)
    query = f"SELECT Name, Type, Total FROM {table_name} ORDER BY Total DESC LIMIT 10"
    result_df = spark.sql(query)
    assert result_df.count() == 10, "SQL query did not return 10 results"
    assert "Name" in result_df.columns, "'Name' column is missing in query result"
    print("Spark SQL query function passed.")


# Function to test Databricks API credentials (optional)
def test_databricks_credentials():
    if not server_h or not access_token:
        pytest.skip(
            "Databricks server hostname or access token not set in environment."
        )
    headers = {"Authorization": f"Bearer {access_token}"}
    # Simple API call to check authentication
    url = f"https://{server_h}/api/2.0/clusters/list"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        assert (
            response.status_code == 200
        ), "Failed to authenticate with Databricks API."
        print("Databricks credentials are valid.")
    except requests.exceptions.HTTPError as err:
        pytest.fail(f"HTTP Error: {err}")
    except Exception as e:
        pytest.fail(f"Error: {e}")
