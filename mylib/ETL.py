import requests
from dotenv import load_dotenv
import os
import json
import base64
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws, monotonically_increasing_id

# Load environment variables
load_dotenv()
server_hostname = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/mini_project"
headers = {"Authorization": f"Bearer {access_token}"}
url = f"https://{server_hostname}/api/2.0"


def perform_query(path, headers, data={}):
    session = requests.Session()
    resp = session.request(
        "POST", url + path, data=json.dumps(data), verify=True, headers=headers
    )
    return resp.json()


def mkdirs(path, headers):
    _data = {"path": path}
    return perform_query("/dbfs/mkdirs", headers=headers, data=_data)


def create(path, overwrite, headers):
    _data = {"path": path, "overwrite": overwrite}
    return perform_query("/dbfs/create", headers=headers, data=_data)


def add_block(handle, data, headers):
    _data = {"handle": handle, "data": data}
    return perform_query("/dbfs/add-block", headers=headers, data=_data)


def close(handle, headers):
    _data = {"handle": handle}
    return perform_query("/dbfs/close", headers=headers, data=_data)


def put_file_from_url(url, dbfs_path, overwrite, headers):
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content
        handle = create(dbfs_path, overwrite, headers=headers)["handle"]
        print("Putting file: " + dbfs_path)
        for i in range(0, len(content), 2**20):
            add_block(
                handle,
                base64.standard_b64encode(content[i : i + 2**20]).decode(),
                headers=headers,
            )
        close(handle, headers=headers)
        print(f"File {dbfs_path} uploaded successfully.")
    else:
        print(f"Error downloading file from {url}. Status code: {response.status_code}")


def extract(
    url="https://gist.githubusercontent.com/armgilles/194bcff35001e7eb53a2a8b441e8b2c6/raw/92200bc0a673d5ce2110aaad4544ed6c4010f687/pokemon.csv",
    file_path=FILESTORE_PATH + "/pokemon.csv",
    directory=FILESTORE_PATH,
    overwrite=True,
):
    """Extracts a URL to a file path on DBFS."""
    # Make the directory
    mkdirs(path=directory, headers=headers)
    # Upload the CSV file to DBFS
    put_file_from_url(url, file_path, overwrite, headers=headers)
    return file_path


"""
Transform and Load function
"""


def load(dataset=FILESTORE_PATH + "/pokemon.csv", table_name="pokemon_data"):
    """Transforms and loads data into a Delta table."""
    spark = SparkSession.builder.appName(
        "Transform and Load Pokemon Data"
    ).getOrCreate()
    # Load data from DBFS
    df_spark = spark.read.csv(dataset, header=True, inferSchema=True)

    # Transformations
    df_spark = df_spark.withColumn("Type", concat_ws("/", col("Type 1"), col("Type 2")))
    df_spark = df_spark.withColumn(
        "Type", when(col("Type 2").isNull(), col("Type 1")).otherwise(col("Type"))
    )
    df_spark = df_spark.withColumnRenamed("#", "Number")
    df_spark = df_spark.withColumnRenamed("Sp. Atk", "Sp_Atk")
    df_spark = df_spark.withColumnRenamed("Sp. Def", "Sp_Def")
    # Add unique ID
    df_spark = df_spark.withColumn("id", monotonically_increasing_id())
    # Select relevant columns
    df_spark = df_spark.select(
        "id",
        "Number",
        "Name",
        "Type",
        "Total",
        "HP",
        "Attack",
        "Defense",
        "Sp_Atk",
        "Sp_Def",
        "Speed",
        "Generation",
        "Legendary",
    )

    # Save as Delta table
    df_spark.write.format("delta").mode("overwrite").saveAsTable(table_name)
    num_rows = df_spark.count()
    print(f"Number of rows in the transformed dataset: {num_rows}")
    return "Transformation and loading completed successfully."


"""
Query function
"""


def query_transform(table_name="pokemon_data"):
    """Runs a query on the transformed data."""
    spark = SparkSession.builder.appName("Run Query").getOrCreate()
    query = f"""
        SELECT Type, COUNT(*) AS count
        FROM {table_name}
        GROUP BY Type
        ORDER BY count DESC
    """
    query_result = spark.sql(query)
    query_result.show(10)
    num_rows = query_result.count()
    print(f"Total number of types: {num_rows}")
    return query_result


if __name__ == "__main__":
    # extract()
    load()
    # query_transform()
