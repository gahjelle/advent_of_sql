"""Advent of SQL, day 4, 2024: The Great Toy Tag Migration of 2024."""

import polars as pl

import aosql


def solve():
    """Solve the puzzle."""
    return (
        pl.scan_parquet(aosql.table("toy_production"))
        .with_columns(
            added=pl.col("new_tags").list.set_difference("previous_tags"),
            unchanged=pl.col("new_tags").list.set_intersection("previous_tags"),
            removed=pl.col("previous_tags").list.set_difference("new_tags"),
        )
        .select(
            pl.col("toy_id"),
            num_added=pl.col("added").list.len(),
            num_unchanged=pl.col("unchanged").list.len(),
            num_removed=pl.col("removed").list.len(),
        )
        .sort(by=pl.col("num_added"), descending=True)
        .head(1)
    ).collect()


if __name__ == "__main__":
    print(aosql.output(solve()))
