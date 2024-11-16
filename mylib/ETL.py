"""
Extract a dataset from a URL like Kaggle or data.gov. 
JSON or CSV formats tend to work well
"""

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, when
import os


def log_output(operation, output, query=None):
    LOG_FILE = "pyspark_output.md"
    """Adds log information to a markdown file."""
    with open(LOG_FILE, "a") as file:
        file.write(f"## {operation}\n\n")
        if query:
            file.write(f"### Query:\n```\n{query}\n```\n\n")
        file.write("### Output:\n\n")
        file.write(f"```\n{output}\n```\n\n")


def extract(
    spark,
    url="https://gist.githubusercontent.com/armgilles/194bcff35001e7eb53a2a8b441e8b2c6/raw/92200bc0a673d5ce2110aaad4544ed6c4010f687/pokemon.csv",
):
    log_output("Initiating Extract", "Fetching data from URL.")
    # Request data from API
    response = requests.get(url)
    if response.status_code == 200:
        csv_path = os.path.join("data", "pokemon.csv")
        with open(csv_path, "wb") as f:
            f.write(response.content)
        # Load data into a Spark DataFrame
        df_spark = spark.read.csv("data/pokemon.csv", header=True, inferSchema=True)
        output = df_spark.limit(10).toPandas().to_markdown()
        log_output("Data Extracted", output)
        print("Data extracted successfully.", output)
        return df_spark
    else:
        error_message = f"Failed to fetch data. Status code: {response.status_code}"
        log_output("Extract Failed", error_message)
        print(error_message)
        return None


def transform(df_spark):
    log_output("Initiating Transform", "Transforming data...")
    # Combine 'Type 1' and 'Type 2' into a new 'Type' column
    df_spark = df_spark.withColumn("Type", concat_ws("/", col("Type 1"), col("Type 2")))
    # Replace null Type 2 with Type 1
    df_spark = df_spark.withColumn(
        "Type", when(col("Type 2").isNull(), col("Type 1")).otherwise(col("Type"))
    )
    # Rename '#' column to 'Number' for clarity
    df_spark = df_spark.withColumnRenamed("#", "Number")
    df_spark = df_spark.withColumnRenamed("Sp. Atk", "Sp_Atk")
    df_spark = df_spark.withColumnRenamed("Sp. Def", "Sp_Def")
    # Select relevant columns
    df_spark = df_spark.select(
        col("Number"),
        col("Name"),
        col("Type"),
        col("Total"),
        col("HP"),
        col("Attack"),
        col("Defense"),
        col("Sp_Atk"),
        col("Sp_Def"),
        col("Speed"),
        col("Generation"),
        col("Legendary"),
    )
    output = df_spark.limit(20).toPandas().to_markdown()
    log_output("Data Transformed", output)
    print("Data transformed successfully.")
    return df_spark


def load(df_spark, table_name):
    """Loads the transformed data into a Databricks SQL table."""
    try:
        # Write the DataFrame to Databricks as a managed table
        df_spark.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"Data loaded into {table_name} successfully.")
        log_output("Data Load", f"Data loaded into {table_name} successfully.")
    except Exception as e:
        error_message = f"Error loading data: {e}"
        log_output("Load Failed", error_message)
        print(error_message)


def spark_sql_query(spark, query):
    log_output("Initiating SQL Query", "Executing SQL query on Spark DataFrame.")
    # Execute the SQL query and log the results
    result_df = spark.sql(query)
    output = result_df.limit(10).toPandas().to_markdown()
    log_output("SQL Query Executed", output, query=query)
    print("Spark SQL query executed successfully.")


# Usage example:
if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("ETL Pokemon Data").getOrCreate()
    df_spark = extract(spark)
    if df_spark:
        df_spark = transform(df_spark)
        table_name = "pokemon_data"
        load(df_spark, table_name)
        # spark_sql_query(spark, f"SELECT * FROM {table_name} LIMIT 10")
