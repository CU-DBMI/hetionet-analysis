
"""
Module for dealing with SQL-specific operations
"""

import gzip

def extract_and_write_sql_block(
    sql_file: str, sql_start: str, sql_end: str, output_file: str
) -> bool:
    """
    Extracts a block of SQL statements from a compressed SQL dump file
    and writes it to a specified output file.

    Args:
        sql_file (str): The path to the compressed SQL dump file (e.g., .sql.gz).
        sql_start (str): The start pattern to identify the beginning of the SQL block.
        sql_end (str): The end pattern to identify the end of the SQL block.
        output_file (str): The path to the output file where the block will be written.

    Returns:
        bool: True if the SQL block was successfully written to the file, False if the block was not found.
    """
    with gzip.open(sql_file, "rt") as f:
        in_sql_block = False  # Flag to track whether we are inside the block
        with open(output_file, "w") as out_file:
            for line in f:
                if sql_start in line:
                    in_sql_block = True  # Start collecting the SQL block

                if in_sql_block:
                    out_file.write(line)  # Write each line directly to the output file

                if in_sql_block and sql_end in line:  # End of the SQL block
                    return output_file  # Block successfully written

    return None  # Return False if no block was found
