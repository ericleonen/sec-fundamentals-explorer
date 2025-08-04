from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import udf, col, row_number, desc, lit, substring, sum
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

METRIC_TAG_MAP = {
    "profits": "NetIncomeLoss",
    "assets": "Assets",
    "stockholders_equity": "StockholdersEquity",
    "capital_expenditures": "CapitalExpenditures"
}
METRIC_TAGS = list(METRIC_TAG_MAP.values())

def preprocess_raw_financial_statements(
        source_path: str, 
        target_path: str, 
        industries_filepath: str,
        test_mode: bool = False
):
    """
    Preprocesses raw SEC financial statements from the source into flat CSV data into the target.
    Data is given per industry.

    Parameters
    ----------

    Returns
    """
    spark = SparkSession.builder \
        .appName("preprocess_raw_financial_statements") \
        .master("local[*]") \
        .getOrCreate()
    
    union_df = None

    for dirname in os.listdir(source_path):
        if test_mode and not dirname.startswith("2020"):
            continue

        print(f"Processing {dirname}:")
        print(" > Reading...")

        num_df = spark.read.csv(
            path=os.path.join(source_path, dirname, "num.txt"),
            sep="\t",
            header=True,
            inferSchema=True
        ) \
            .filter(col("version").startswith("us-gaap")) \
            .filter(col("value").isNotNull()) \
            .filter(col("segments").isNull()) \
            .filter(col("coreg").isNull()) \
            .filter(col("uom") == "USD") \
            .filter(col("qtrs").isin([0, 4])) \
            .filter(col("tag").isin(METRIC_TAGS)) \
            .select("adsh", "tag", "ddate", "value")

        sub_df = spark.read.csv(
            path=os.path.join(source_path, dirname, "sub.txt"),
            sep="\t",
            header=True,
            inferSchema=True
        ) \
            .filter(col("prevrpt") == 0) \
            .filter(col("form") == "10-K") \
            .filter(col("sic").isNotNull()) \
            .withColumn("sic", col("sic").cast("int")) \
            .select("adsh", "cik", "sic", "filed")

        print(" > Joining...")

        joined_df = num_df.join(sub_df, on="adsh", how="inner").drop("adsh")

        print(" > Unioning...")

        union_df = joined_df if union_df is None else union_df.union(joined_df)

    print("Deduping...")

    window = Window.partitionBy("cik", "tag","ddate").orderBy(desc("filed"))
    deduped_df = union_df.withColumn("row_num", row_number().over(window)) \
                    .filter("row_num = 1").drop("row_num")
    
    def industry_mapping_func(industries_filepath: str):
        industries_map = {}

        with open(industries_filepath, "r") as industries_file:
            current_industry = None

            for line in industries_file:
                line = line.strip()

                if line.endswith(":"):
                    current_industry = line[:-1]
                    industries_map[current_industry] = []
                elif len(line) > 0:
                    min_sic, max_sic = map(int, line.split("-"))
                    industries_map[current_industry].append((min_sic, max_sic))

        def map_sic_to_industry(sic: int) -> str:
            for industry, ranges in industries_map.items():
                for min_sic, max_sic in ranges:
                    if min_sic <= sic <= max_sic:
                        return industry
            return "Other"

        return map_sic_to_industry

    sic_mapper_udf = udf(
        industry_mapping_func(industries_filepath), 
        StringType()
    )

    print("Cleaning...")

    final_df = deduped_df.withColumn("year", substring("ddate", 1, 4)) \
                    .withColumn("industry", sic_mapper_udf("sic")) \
                    .select("tag", "industry", "year", "value")

    print("Writing...")
    overwrite = True
    for metric, tag in METRIC_TAG_MAP.items():
        metric_df = final_df.filter(col("tag") == tag) \
            .groupBy("year", "industry") \
            .agg(sum("value").alias("value")) \
            .withColumn("metric", lit(metric))
        
        metric_df \
            .write.mode("overwrite" if overwrite else "append") \
            .csv(target_path, header=True)
        
        overwrite = False
        
if __name__ == "__main__":
    preprocess_raw_financial_statements(
        source_path="/workspace/data/raw/",
        target_path="/workspace/data/preprocessed/",
        industries_filepath="/workspace/data/fama-french-industries.txt",
        test_mode=False
    )