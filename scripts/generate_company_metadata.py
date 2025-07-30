from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, to_date
from pyspark.sql.window import Window
import os

def generate_company_metadata(source: str, save_to: str):
    """
    Generate metadata for companies from raw financial statements.

    Parameters
    ----------
    source: str
        Directory path where the raw financial statement data is.
    save_to: str
        Directory path where the generated company metadata will be placed.

    Returns
    -------
    None
    """

    spark = SparkSession.builder \
                .appName("generate_company_metadata") \
                .master("local[*]") \
                .getOrCreate()

    print(f"Processing directories from {source}:")

    sub_df = None

    for index, dirname in enumerate(os.listdir(source)):
        print(f" - {dirname}")

        new_sub_df = spark.read.csv(
            path=f"{source}{dirname}/sub.txt",
            sep="\t",
            header=True,
            inferSchema=True
        ) \
            .withColumn("filed", to_date("filed", "yyyyMMdd")) \

        if sub_df is None:
            sub_df = new_sub_df
        else:
            sub_df = sub_df.union(new_sub_df)

    window = Window.partitionBy("cik").orderBy(col("filed").desc())
    latest_df = sub_df.withColumn("row_num", row_number().over(window)) \
        .filter(col("row_num") == 1) \
        .select("cik", "name", "sic", "countryba", "stprba", "cityba", "bas1", "bas2")
    print(" > Deduped")

    latest_df.write.mode("overwrite").csv(
        path=save_to,
        header=True
    )
    print(" > Appended CSV")

    print(f"Successfully wrote to {save_to}.")
    
    spark.stop()

if __name__ == "__main__":
    generate_company_metadata(
        source="data/raw/",
        save_to="data/preprocessed/company_metadata/"
    )