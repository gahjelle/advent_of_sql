"""Advent of SQL, day 2, 2024: Santa's Jumbled Letters"""

import string

import polars as pl

import aosql

TO_CHARS = {ord(char): char for char in f"{string.ascii_letters} ,.!"}


def solve():
    """Solve the puzzle."""
    return (
        (
            pl.concat(
                [
                    pl.scan_parquet(aosql.table("letters_a")),
                    pl.scan_parquet(aosql.table("letters_b")),
                ]
            )
            .sort(by="id")
            .select(
                char=pl.col("value").replace_strict(
                    TO_CHARS, default="", return_dtype=pl.String
                )
            )
            .filter(pl.col("char") != "")
        )
        .collect()
        .transpose()
        .select(pl.concat_str(pl.all()))
    )


if __name__ == "__main__":
    print(aosql.output(solve()))
