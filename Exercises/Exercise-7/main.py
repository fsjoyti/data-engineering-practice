import os
import zipfile
from itertools import combinations
from os import path
from zipfile import ZipFile

import pandas as pd
import pyspark.sql.functions as F
from pyspark import StorageLevel
from pyspark.sql import SparkSession, types
from pyspark.sql.types import DateType
from pyspark.sql.window import Window


def get_file_paths(root_directory, folder_name, file_extension):
    file_paths = []
    file_directory = os.path.join(root_directory, folder_name)
    for root, _, files in os.walk(file_directory):
        for file_name in files:
            if file_extension in file_name:
                file_paths.append(os.path.join(root, file_name))
    return file_paths


def read_zip_files_into_dataframes(zip_file_paths):
    df_list = []
    for file_path in zip_file_paths:
        print(f"Reading for {file_path}")
        zip_file = ZipFile(file_path)
        for text_file in zip_file.infolist():
            if text_file.filename.endswith(
                ".csv"
            ) and not text_file.filename.startswith("__MACOSX"):
                df = pd.read_csv(zip_file.open(text_file.filename))
                print(text_file.filename)
                df["source_file"] = text_file.filename
                df.head()
                df_list.append(df)
    return df_list


def find_minimal_index(df):
    for n in range(1, len(df.columns) + 1):
        for c in combinations(df.columns, n):
            if not df[list(c)].duplicated().any():
                return df[list(c)].columns


def add_file_date_column(df):
    date_format = "yyyy-MM-dd"
    return df.withColumn(
        "file_date",
        F.to_date(
            F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1), date_format
        ),
    )


def add_brand_column(df):
    return df.withColumn(
        "brand",
        F.when(
            F.col("model").contains(" "), F.split(F.col("model"), " ").getItem(0)
        ).otherwise(F.col("model")),
    )


def add_storage_ranking(df):
    # Cache the DataFrame to avoid recomputation if used multiple times
    df = df.select("model", "capacity_bytes").distinct().cache()
    # Convert capacity_bytes to integer
    df = df.withColumn(
        "capacity_bytes", F.col("capacity_bytes").cast(types.IntegerType())
    )
    window_spec = Window.partitionBy("model").orderBy(F.desc("capacity_bytes"))
    ranked_df = df.withColumn("storage_ranking", F.dense_rank().over(window_spec))
    return ranked_df


def _add_storage_ranking_column(df, ranked_df):
    return df.join(F.broadcast(ranked_df), on="model", how="left")


def add_primary_key_column(df):
    return df.withColumn("primary_key", F.hash("serial_number"))


def main():
    pd.DataFrame.iteritems = pd.DataFrame.items
    current_directory = os.getcwd()
    zip_file_paths = get_file_paths(current_directory, r"data", "zip")
    print(zip_file_paths)
    pdf = read_zip_files_into_dataframes(zip_file_paths)
    print(pdf[0].head())
    df = pdf[0]
    spark = (
        SparkSession.builder.appName("Exercise7")
        .enableHiveSupport()
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "2g")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    print(spark.version)
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    # your code here
    sparkDF = spark.createDataFrame(df)
    sparkDF.persist(StorageLevel.MEMORY_AND_DISK)
    sparkDF = sparkDF.repartition("model")
    new_df = add_file_date_column(sparkDF)
    new_df = add_brand_column(new_df)
    ranked_df = add_storage_ranking(new_df)
    final_df = _add_storage_ranking_column(new_df, ranked_df)
    final_df = add_primary_key_column(final_df)
    print(final_df.explain(mode="extended"))
    print(final_df.show())
    # close the Spark session
    spark.stop()
    # close the zip file
    for file_path in zip_file_paths:
        with ZipFile(file_path) as zip_file:
            zip_file.close()


if __name__ == "__main__":
    main()
