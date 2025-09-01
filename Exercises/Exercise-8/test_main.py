import os
from unittest.mock import AsyncMock, patch

import aioduckdb
import pytest
from main import (
    execute_sql_file,
    get_env_path,
    get_insert_query,
    insert_from_csv,
    setup_extension,
    truncate_table,
)


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
