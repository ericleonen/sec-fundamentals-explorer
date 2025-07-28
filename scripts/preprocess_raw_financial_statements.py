"""
Script that preprocesses financial statement data from data/raw into a Parquet files.
"""

from pyspark.sql import SparkSession, DataFrame, Window
import os
from functools import reduce
from pyspark.sql.functions import col, to_date, row_number
from pyspark.sql.types import DecimalType, LongType
import shutil

def preprocess_financial_statements(source: str, save_to: str):
    """
    Preprocesses all num.txt and sub.txt from the given source directory into a flat CSV\. Columns
    are cik, tag, ddate, and value.

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

    num_df = reduce(DataFrame.unionByName, num_dfs) \
                .withColumn("ddate", to_date("ddate", "yyyyMMdd")) \
                .select("adsh", "tag", "ddate", "value", "uom", "coreg", "segments")
                
    sub_df = reduce(DataFrame.unionByName, sub_dfs) \
                .withColumn("filed", to_date("filed", "yyyyMMdd")) \
                .select("adsh", "cik", "filed")

    joined_df = num_df.join(sub_df, on="adsh")

    filtered_df = joined_df.filter(
        (col("coreg").isNull()) &
        (col("segments").isNull()) &
        (col("uom") == "USD") &
        (col("value").isNotNull())
    )

    window = Window.partitionBy("cik", "tag", "ddate").orderBy(col("filed").desc())
    ranked_df = filtered_df.withColumn("row_num", row_number().over(window))
    deduped_df = ranked_df.filter(col("row_num") == 1)

    final_df = deduped_df.select("cik", "tag", "ddate", "value")

    final_df.write.csv(save_to, header=True, mode="overwrite")
    
    print(f"Successfully wrote to {save_to}.")

if __name__ == "__main__":
    preprocess_financial_statements(
        source="data/raw/",
        save_to="data/preprocessed/"
    )