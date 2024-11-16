from pyspark.sql import SparkSession
from mylib.ETL import extract, transform, load
from mylib.query import spark_sql_query  # Assuming spark_sql_query is in query_db.py


def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("ETL Pokemon Data").getOrCreate()

    # URL of the Pokémon CSV data
    url = (
        "https://gist.githubusercontent.com/armgilles/"
        "194bcff35001e7eb53a2a8b441e8b2c6/"
        "raw/92200bc0a673d5ce2110aaad4544ed6c4010f687/pokemon.csv"
    )
    # Extract, transform, and load data
    df_spark = extract(spark, url)
    if df_spark:
        df_spark = transform(df_spark)
        table_name = "pokemon_data"
        load(df_spark, table_name)

        # Execute SQL queries
        # Example: Select all legendary Pokémon
        query1 = (
            f"SELECT Name, Type, Total FROM {table_name} "
            f"WHERE Legendary = 'True' ORDER BY Total DESC"
        )
        spark_sql_query(spark, query1)

        # Example: Select top 10 Pokémon by Total stats
        query2 = (
            f"SELECT Name, Type, Total FROM {table_name} ORDER BY Total DESC LIMIT 10"
        )
        spark_sql_query(spark, query2)


if __name__ == "__main__":
    main()
