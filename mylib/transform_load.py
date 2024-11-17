"""
Transform and Load function
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, concat_ws, col, when
FILESTORE_PATH = "dbfs:/FileStore/SiMinL_Week11"

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


if __name__ == "__main__":
    load()
