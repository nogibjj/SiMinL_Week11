from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DatabricksDirectQuery").getOrCreate()


def spark_sql_query(query):
    result_df = spark.sql(query)
    print("Spark SQL executed:\n", result_df.limit(10).toPandas().to_markdown())
