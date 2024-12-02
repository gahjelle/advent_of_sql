"""Utilities to help with Advent of SQL in DataFrames"""

import inspect
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent.parent.parent


def table(table_name: str) -> Path:
    """Convert table name to a parquet data path

    Use inspect to "magically" choose the correct parquet file based on the
    directory of the file calling this function.
    """
    caller_path = Path(inspect.currentframe().f_back.f_code.co_filename)
    year = caller_path.parent.parent.name
    day = int(caller_path.parent.name.split("_")[0])
    return BASE_DIR / "data" / year / f"aosql{year}{day:02d}_{table_name}.parquet"


def output(table):
    """Print a dataframe comma-separated, line-by-line."""
    return "\n".join(",".join(str(field) for field in row) for row in table.iter_rows())
