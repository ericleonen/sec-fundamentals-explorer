"""
Script that preprocesses financial statement data from data/raw into a Parquet files.
"""

from pyspark.sql import SparkSession, DataFrame, Window
import os
from functools import reduce
from pyspark.sql.functions import col, to_date, row_number

def preprocess_financial_statements(
        ciks: list[str],
        source: str, 
        save_to: str
):
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

    print(f"Processing directories from {source}:")

    int_ciks = [int(cik) for cik in ciks]
    union_df = None

    for dirname in os.listdir(source):
        print(f" - {dirname}")

        num_df = spark.read.csv(
            path=f"{source}{dirname}/num.txt",
            sep="\t",
            header=True,
            inferSchema=True
        ) \
            .filter(col("version").startswith("us-gaap")) \
            .filter(col("uom") == "USD") \
            .filter(col("value").isNotNull()) \
            .filter(col("segments").isNull()) \
            .filter(col("coreg").isNull()) \
            .select("adsh", "tag", "ddate", "qtrs", "value")

        sub_df = spark.read.csv(
            path=f"{source}{dirname}/sub.txt",
            sep="\t",
            header=True,
            inferSchema=True
        ) \
            .filter(col("cik").isin(int_ciks)) \
            .filter(col("prevrpt") == 0) \
            .filter(col("form") != "8-K") \
            .select("adsh", "cik", "filed")

        joined_df = num_df.join(sub_df, on="adsh", how="inner").drop("adsh")
        print("   > Joined")

        if union_df is None:
            union_df = joined_df
        elif not joined_df.isEmpty():
            union_df = union_df.union(joined_df)
        print("   > Unioned")

    window = Window.partitionBy("cik", "tag", "ddate", "qtrs").orderBy(col("filed").desc())
    deduped_df = union_df.withColumn("row_num", row_number().over(window)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    deduped_df.write.mode("overwrite").csv(
        path=save_to,
        header=True,
        sep="\t"
    )

    print(f"Successfully wrote to {save_to}.")

    spark.stop()

if __name__ == "__main__":
    MAG7_CIKS = [
        "0000320193",  # Apple Inc.
        "0000789019",  # Alphabet Inc.
        "0001018724",  # Amazon.com, Inc.
        "0001318605",  # Meta Platforms, Inc.
        "0001652044",  # Microsoft Corporation
        "0001326801",  # Tesla, Inc.
        "0001045810",  # NVIDIA Corporation
    ]

    preprocess_financial_statements(
        ciks=MAG7_CIKS,
        source="data/raw/",
        save_to="data/preprocessed/financial_statements/"
    )