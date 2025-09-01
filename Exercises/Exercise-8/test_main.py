import os
import shutil
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import aioduckdb
import pytest
from main import (
    execute_sql_file,
    get_env_path,
    get_insert_query,
    get_most_popular_vehicle_postal_code,
    get_number_of_cars_by_model_year,
    get_number_of_cars_per_city,
    get_top_three_make_model,
    insert_from_csv,
    query_to_csv,
    query_to_parquet,
    setup_extension,
    truncate_table,
)


@pytest.mark.asyncio
async def test_truncate_table():
    async with aioduckdb.connect(database=":memory:") as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS test_table (id INTEGER, name VARCHAR);"
        )
        await conn.execute(
            "INSERT INTO test_table (id, name) VALUES (1, 'Alice'), (2, 'Bob');"
        )
        result_before = await conn.execute("SELECT * FROM test_table;")
        rows_before = await result_before.fetchall()
        assert len(rows_before) == 2
        await truncate_table(conn, "test_table")
        result_after = await conn.execute("SELECT * FROM test_table;")
        rows_after = await result_after.fetchall()
        assert len(rows_after) == 0


@pytest.mark.asyncio
@patch("main.get_insert_query")
@patch("main.truncate_table", new_callable=AsyncMock)
async def test_insert_from_csv(mock_truncate, mock_get_insert_query):
    mock_get_insert_query.return_value = (
        "INSERT INTO test_table SELECT * FROM 'test_data.csv'"
    )
    async with aioduckdb.connect(database=":memory:") as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS test_table (id INTEGER, name VARCHAR);"
        )
        await insert_from_csv(conn, "test_table", "test_data.csv")
        result_obj = await conn.execute("SELECT * FROM test_table;")
        result = await result_obj.fetchall()
        assert len(result) > 0
        mock_truncate.assert_awaited_once()
        mock_get_insert_query.assert_called_once_with("test_table", "test_data.csv")


@pytest.mark.asyncio
async def test_execute_sql_file():
    async with aioduckdb.connect(database=":memory:") as conn:
        sql_file_path = os.path.join(os.path.dirname(__file__), "test_sql_script.sql")
        table_name = await execute_sql_file(conn, [sql_file_path])
        assert table_name == "test_table"
        result_obj = await conn.execute("SELECT * FROM test_table;")
        result = await result_obj.fetchall()
        assert len(result) > 0


@pytest.mark.asyncio
async def test_get_env_path():
    current_directory = os.path.dirname(__file__)
    test_file_path = os.path.join(current_directory, "test_data.csv")
    result = get_env_path(current_directory, ".csv")
    assert test_file_path in result


@pytest.mark.asyncio
async def test_query_to_csv_and_parquet():
    # Create a temporary directory that will be cleaned up automatically
    with tempfile.TemporaryDirectory() as tmp_dir:
        csv_output_path = Path(tmp_dir) / "test_output.csv"
        parquet_output_path = Path(tmp_dir) / "test_output.parquet"

        async with aioduckdb.connect(database=":memory:") as conn:
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS test_table (id INTEGER, name VARCHAR);"
            )
            await conn.execute(
                "INSERT INTO test_table (id, name) VALUES (1, 'Alice'), (2, 'Bob');"
            )
            await query_to_csv(conn, "SELECT * FROM test_table;", str(csv_output_path))
            assert csv_output_path.exists()
            await query_to_parquet(
                conn,
                "SELECT * FROM test_table;",
                str(parquet_output_path),
                partition_cols=None,
            )
            assert parquet_output_path.exists()

        # The temporary directory and its contents will be automatically cleaned up


@pytest.mark.asyncio
@patch("main.query_to_csv", new_callable=AsyncMock)
async def test_get_number_of_cars_per_city(query_to_csv_mock):
    async with aioduckdb.connect(database=":memory:") as conn:
        output_path = Path(tempfile.gettempdir()) / "cars_per_city.csv"
        await get_number_of_cars_per_city(conn, "test_table", str(output_path))
        query_to_csv_mock.assert_awaited_once()
        query_to_csv_mock.assert_awaited_with(
            conn,
            """SELECT City, COUNT(*) as Number_Of_Cars FROM test_table GROUP BY City ORDER BY COUNT(*) DESC;""",
            str(output_path),
        )


@pytest.mark.asyncio
@patch("main.query_to_csv", new_callable=AsyncMock)
async def test_get_top_three_make_model(query_to_csv_mock):
    async with aioduckdb.connect(database=":memory:") as conn:
        output_path = Path(tempfile.gettempdir()) / "top_three_make_model.csv"
        await get_top_three_make_model(conn, "test_table", str(output_path))
        query_to_csv_mock.assert_awaited_once()
        query_to_csv_mock.assert_awaited_with(
            conn,
            """
    WITH make_model_count AS (
        SELECT Make, Model, COUNT(*) as cnt FROM test_table GROUP BY Make, Model
    )
    SELECT Make, Model FROM make_model_count ORDER BY cnt DESC LIMIT 3;
    """,
            str(output_path),
        )


@pytest.mark.asyncio
@patch("main.query_to_csv", new_callable=AsyncMock)
async def test_get_most_popular_vehicle_postal_code(query_to_csv_mock):
    async with aioduckdb.connect(database=":memory:") as conn:
        output_path = (
            Path(tempfile.gettempdir()) / "most_popular_vehicle_postal_code.csv"
        )
        await get_most_popular_vehicle_postal_code(conn, "test_table", str(output_path))
        query_to_csv_mock.assert_awaited_once()
        query_to_csv_mock.assert_awaited_with(
            conn,
            """
    WITH ranked AS (
        SELECT "Postal_Code", Make, Model, COUNT(*) as cnt,
               ROW_NUMBER() OVER (PARTITION BY "Postal_Code" ORDER BY COUNT(*) DESC) as rn
        FROM test_table
        GROUP BY "Postal_Code", Make, Model
    )
    SELECT "Postal_Code", Make, Model FROM ranked WHERE rn = 1;
    """,
            str(output_path),
        )

@pytest.mark.asyncio
@patch("main.query_to_parquet", new_callable=AsyncMock)
async def test_get_number_of_cars_by_model_year(query_to_parquet_mock):
    async with aioduckdb.connect(database=":memory:") as conn:
        output_path = Path(tempfile.gettempdir()) / "cars_by_model_year"
        await get_number_of_cars_by_model_year(conn, "test_table", str(output_path))
        query_to_parquet_mock.assert_awaited_once()
        query_to_parquet_mock.assert_awaited_with(
            conn,
            """
    SELECT Make, Model, "Model_Year" as Year, COUNT(*) as Number_Of_Cars
    FROM test_table
    GROUP BY Make, Model, "Model_Year"
    ORDER BY COUNT(*) DESC;
    """,
            str(output_path),
            partition_cols=["Year"],
        )

@pytest.mark.asyncio
async def test_setup_extension():
    async with aioduckdb.connect(database=":memory:") as conn:
        await setup_extension(conn, "httpfs")
        result = await conn.execute("SELECT * FROM duckdb_extensions();")
        extensions = await result.fetchall()
        assert any("httpfs" in ext for ext in extensions)