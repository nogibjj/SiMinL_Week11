"""
Query function
"""
from pyspark.sql import SparkSession


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
    query_transform()