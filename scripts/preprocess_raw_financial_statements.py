"""
Script that preprocesses financial statement data from data/raw into a Parquet files.
"""

from pyspark.sql import SparkSession, DataFrame
import os
from functools import reduce
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DecimalType, LongType

def preprocess_financial_statements(source: str, save_to: str) -> DataFrame:
    """
    Preprocesses all num.txt and sub.txt from the given source directory into Parquets. Columns
    are cik, name, tag, ddate, and value.

    Parameters
    ----------
    source: str
        Directory path where the raw financial statement data is.
    save_to: str
        Directory path where preprocesssed financial statement data will be placed.
    
    Returns
    -------
    None
    """

    spark = SparkSession.builder \
                .appName("preprocess_raw_financial_statements") \
                .master("local[*]") \
                .getOrCreate()
    
    num_dfs, sub_dfs = [], []

    print(f"Reading directories from {source}:")
    for dirname in os.listdir(source):
        print(f" - {dirname}")

        num_dfs.append(spark.read.csv(
            path=f"{source}{dirname}/num.txt",
            sep="\t",
            header=True,
            inferSchema=True
        ))

        sub_dfs.append(spark.read.csv(
            path=f"{source}{dirname}/sub.txt",
            sep="\t",
            header=True,
            inferSchema=True
        ))

    num_df = reduce(DataFrame.unionByName, num_dfs)
    sub_df = reduce(DataFrame.unionByName, sub_dfs)

    num_df = num_df \
                .select("adsh", "tag", "ddate", "value") \
                .withColumn("ddate", to_date("ddate", "yyyyMMdd")) \
                .withColumn("value", col("value").cast(DecimalType(28, 4)))

    sub_df = sub_df \
                .select("adsh", "cik", "name", "form", "period") \
                .withColumn("cik", col("cik").cast(LongType())) \
                .withColumn("period", to_date("period", "yyyyMMdd"))

    joined_df = sub_df \
        .join(num_df, on="adsh", how="inner") \
        .select("cik", "name", "tag", "ddate", "value")

    joined_df.coalesce(1).write.csv(save_to, header=True, mode="overwrite")

    for filename in os.listdir(save_to):
        if filename.startswith("part-") and filename.endswith(".csv"):
            
    
    print(f"Successfully wrote to {save_to}.")

if __name__ == "__main__":
    preprocess_financial_statements(
        source="data/raw/",
        save_to="data/preprocessed/"
    )