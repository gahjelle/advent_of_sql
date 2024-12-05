"""Advent of SQL, 2024 Practice: The Great Christmas Analytics Crisis of 2024"""

import aosql

import polars as pl


def solve():
    """Solve the puzzle.

    TODO: Constraint on max 3 cities per country. Not relevant in data, though
    """
    return (
        pl.scan_parquet(aosql.table("children"))
        .join(pl.scan_parquet(aosql.table("christmaslist")), on="child_id")
        .group_by("city")
        .agg(
            count=pl.col("city").count(),
            naughty_nice_score=pl.col("naughty_nice_score").mean(),
        )
        .filter(pl.col("count") >= 5)
        .sort(by=["naughty_nice_score", "city"], descending=[True, False])
        .head(5)
        .select(pl.col("city"))
    ).collect()


if __name__ == "__main__":
    print(aosql.output(solve()))
