"""
Tests for sql.py
"""

import gzip
import pathlib
import tempfile
from typing import Optional

import pytest

from hetionet_utils.sql import extract_and_write_sql_block


@pytest.mark.parametrize(
    "sql_content, sql_start, sql_end, expected_output, expected_return",
    [
        # Case 1: Successful extraction
        (
            """CREATE TABLE test (
                id INT PRIMARY KEY,
                name TEXT
            );
            INSERT INTO test VALUES (1, 'Alice');""",
            "CREATE TABLE test",
            ");",
            """CREATE TABLE test (
                id INT PRIMARY KEY,
                name TEXT
            );""",
            True,
        ),
        # Case 2: No matching start pattern
        (
            """DROP TABLE test;""",
            "CREATE TABLE test",
            ");",
            None,
            False,
        ),
        # Case 3: No matching end pattern
        (
            """CREATE TABLE test (
                id INT PRIMARY KEY,
                name TEXT
            """,
            "CREATE TABLE test",
            ");",
            None,
            False,
        ),
        # Case 4: Empty file
        (
            "",
            "CREATE TABLE test",
            ");",
            None,
            False,
        ),
        # Case 5: Large content, valid block
        (
            "DROP TABLE old;\n"
            + ("-- filler\n" * 1000)
            + """
            CREATE TABLE test (
                id INT PRIMARY KEY,
                name TEXT
            );
            """,
            "CREATE TABLE test",
            ");",
            """CREATE TABLE test (
                id INT PRIMARY KEY,
                name TEXT
            );""",
            True,
        ),
    ],
)
def test_extract_and_write_sql_block(
    sql_content: str,
    sql_start: str,
    sql_end: str,
    expected_output: Optional[str],
    expected_return: bool,
):
    # Use context managers for temporary files
    with tempfile.NamedTemporaryFile(
        delete=False, suffix=".sql.gz"
    ) as temp_sql_file, tempfile.NamedTemporaryFile(delete=False) as temp_output_file:
        temp_sql_path = pathlib.Path(temp_sql_file.name)
        temp_output_path = pathlib.Path(temp_output_file.name)

    try:
        # Write the SQL content to a gzipped file
        with gzip.open(temp_sql_path, "wt") as gzipped_file:
            gzipped_file.write(sql_content)

        # Run the function
        result = extract_and_write_sql_block(
            str(temp_sql_path), sql_start, sql_end, str(temp_output_path)
        )

        # Assert the return value
        assert result == expected_return

        # If the block was found, assert the output content
        if expected_output:
            output_content = temp_output_path.read_text()
            assert output_content.strip() == expected_output.strip()
        else:
            # If no block was found, the output file should be empty
            output_content = temp_output_path.read_text()
            assert output_content.strip() == ""

    finally:
        # Cleanup
        temp_sql_path.unlink(missing_ok=True)
        temp_output_path.unlink(missing_ok=True)
