"""Advent of SQL, day 6, 2024: Making Presents Fairer."""

import polars as pl

import aosql


def solve():
    """Solve the puzzle."""
    return (
        pl.scan_parquet(aosql.table("children"))
        .join(pl.scan_parquet(aosql.table("gifts")), on="child_id")
        .filter(pl.col("price") > pl.col("price").mean())
        .sort(by="price")
        .head(1)
        .select(pl.col("name"))
    ).collect()


if __name__ == "__main__":
    print(aosql.output(solve()))
