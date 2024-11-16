from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DatabricksDirectQuery").getOrCreate()


def spark_sql_query(query):
    # Execute the SQL query directly using Spark SQL
    result_df = spark.sql(query)
    # Convert the result to Pandas for easy display
    output = result_df.limit(10).toPandas().to_markdown()
    print("Spark SQL query executed successfully:\n", output)


if __name__ == "__main__":
    print("Starting..")
    spark_sql_query("select * from pokemon_data")
