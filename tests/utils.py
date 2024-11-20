"""
Utilities for testing
"""

import contextlib
import pathlib
import tempfile


def sample_generator(data: list[str]):
    """
    Helper function for creating a generator from any iterable data.

    Args:
        data (Any):
            An iterable data structure (e.g., list, tuple, set) containing
            elements to be yielded one by one.

    Yields:
        Any: Each element of the input data, yielded in sequence.
    """
    for item in data:
        yield item


@contextlib.contextmanager
def create_temp_file(content: str):
    """Create a temporary file, yield its path, and clean it up after use."""
    with tempfile.NamedTemporaryFile(delete=False, mode="w") as temp_file:
        try:
            temp_file.write(content)
            temp_file.close()
            yield temp_file.name
        finally:
            pathlib.Path(temp_file.name).unlink()
