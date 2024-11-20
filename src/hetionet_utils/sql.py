"""
Module for dealing with SQL-specific operations
"""

import gzip
import pathlib


def extract_and_write_sql_block(
    sql_file: str, sql_start: str, sql_end: str, output_file: str
) -> bool:
    """
    Extracts a block of SQL statements from a compressed SQL dump file
    and writes it to a specified output file.

    Args:
        sql_file (str):
            The path to the compressed SQL dump file (e.g., .sql.gz).
        sql_start (str):
            The start pattern to identify the beginning of the SQL block.
        sql_end (str):
            The end pattern to identify the end of the SQL block.
        output_file (str):
            The path to the output file where the block will be written.

    Returns:
        bool:
            True if the SQL block was successfully written to the file,
            False if the block was not found or incomplete.
    """
    with gzip.open(sql_file, "rt") as f:
        in_sql_block = False  # Flag to track whether we are inside the block
        temp_content = []  # Temporarily store the lines of the block

        for line in f:
            if sql_start in line:
                in_sql_block = True  # Start collecting the SQL block

            if in_sql_block:
                temp_content.append(line)  # Collect the line

            if in_sql_block and sql_end in line:  # End of the SQL block
                # Write the collected lines to the output file
                with open(output_file, "w") as out_file:
                    out_file.writelines(temp_content)
                return True  # Block successfully written

    # If we exit the loop without finding the end, the block is incomplete
    return False


def remove_first_and_last_line_of_file(target_file: str) -> str:
    """
    Removes the first and last lines of a file.

    This function processes a file, removing its first and last lines.
    It assumes the first line is a header and the last line is a data
    termination line, both of which do not contain actual values.

    The file is modified in place, and a temporary file is used to ensure
    atomic operation.

    Args:
        target_file (str):
            Path to the target file to be processed.

    Returns:
        str:
            Path to the modified file.
    """
    input_file = pathlib.Path(target_file)
    temp_file = input_file.with_suffix(".tmp")

    with input_file.open("r") as infile, temp_file.open("w") as outfile:
        # Skip the first line
        first_line = next(infile, None)

        # Only proceed if the file is not empty
        if first_line is not None:
            # Start with the second line
            prev_line = next(infile, None)
            for line in infile:
                # Write the previous line
                outfile.write(prev_line)
                # Update the previous line buffer
                prev_line = line

            # Note: the last line is in `prev_line` and is not written

    # Replace the original file with the temporary file
    temp_file.replace(input_file)

    return target_file
