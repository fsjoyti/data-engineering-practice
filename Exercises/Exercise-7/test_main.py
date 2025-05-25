import pytest
from main import (
    add_brand_column,
    add_file_date_column,
    add_primary_key_column,
    add_storage_ranking,
)
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    dense_rank,
    desc,
    hash,
    regexp_extract,
    regexp_replace,
    to_date,
)
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType


@pytest.fixture
def spark_fixture():
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark


def assertDataFrameEqual(df1, df2):
    """
    Assert that two DataFrames are equal.
    """
    assert df1.schema == df2.schema, "Schemas are not equal"
    assert df1.count() == df2.count(), "Row counts are not equal"

    for row1, row2 in zip(df1.collect(), df2.collect()):
        for field in df1.schema.fields:
            assert (
                row1[field.name] == row2[field.name]
            ), f"Values are not equal for field {field.name}"
    return True


def test_add_file_date_column(spark_fixture):
    field_name = "source_file"
    input_df = spark_fixture.createDataFrame(
        data=[
            ("sample-file-2024-05-22-test.csv",),
            ("test-file-2025-01-01-first.csv",),
            ("dummy-file-2012-12-31-test.csv",),
        ],
        schema=StructType([StructField(field_name, StringType(), True)]),
    )
    expected_df = input_df.withColumn(
        "file_date",
        to_date(regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1), "yyyy-MM-dd"),
    )
    transformed_df = add_file_date_column(df=input_df)
    assertDataFrameEqual(transformed_df, expected_df)


def test_add_brand_column(spark_fixture):

    field_name = "model"
    input_df = spark_fixture.createDataFrame(
        data=[("Toshiba A",), ("Xerox1113",)],
        schema=StructType([StructField(field_name, StringType(), True)]),
    )
    schemaString = "model brand"
    expected_df = spark_fixture.createDataFrame(
        data=[("Toshiba A", "Toshiba"), ("Xerox1113", "Xerox1113")],
        schema=StructType(
            [
                StructField(field_name, StringType(), True)
                for field_name in schemaString.split()
            ]
        ),
    )
    tranformed_df = add_brand_column(input_df)

    assertDataFrameEqual(tranformed_df, expected_df)


def test_add_storage_ranking(spark_fixture):
    input_df = spark_fixture.createDataFrame(
        data=[
            ("Toshiba A", "1000"),
            ("Toshiba A", "2000"),
            ("Toshiba A", "500"),
            ("Xerox1113", "100"),
            ("Xerox1113", "200"),
            ("ST X", "100"),
            ("ST D", "100"),
        ],
        schema=StructType(
            [
                StructField("model", StringType(), True),
                StructField("capacity_bytes", StringType(), True),
            ]
        ),
    )
    expected_data = [
        ("ST D", 100, 1),
        ("ST X", 100, 1),
        ("Toshiba A", 2000, 1),
        ("Toshiba A", 1000, 2),
        ("Toshiba A", 500, 3),
        ("Xerox1113", 200, 1),
        ("Xerox1113", 100, 2),
    ]

    expected_schema = StructType(
        [
            StructField("model", StringType(), True),
            StructField("capacity_bytes", IntegerType(), True),
            StructField("storage_ranking", IntegerType(), False),
        ]
    )

    expected_df = spark_fixture.createDataFrame(expected_data, schema=expected_schema)

    transformed_df = add_storage_ranking(input_df)
    assertDataFrameEqual(transformed_df, expected_df)


def test_add_primary_key_column(spark_fixture):
    input_df = spark_fixture.createDataFrame(
        data=[
            ("Toshiba A", "1000", "123456"),
            ("Toshiba A", "2000", "654321"),
            ("Toshiba A", "500", "789012"),
            ("Xerox1113", "100", "345678"),
            ("Xerox1113", "200", "987654"),
            ("ST X", "100", "456789"),
            ("ST D", "100", "321654"),
        ],
        schema=StructType(
            [
                StructField("model", StringType(), True),
                StructField("capacity_bytes", StringType(), True),
                StructField("serial_number", StringType(), True),
            ]
        ),
    )

    expected_df = input_df.withColumn("primary_key", hash("serial_number"))

    transformed_df = add_primary_key_column(input_df)
    assertDataFrameEqual(transformed_df, expected_df)
