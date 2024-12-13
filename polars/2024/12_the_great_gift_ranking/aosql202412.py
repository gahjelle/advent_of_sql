"""Advent of SQL, day 12, 2024: The Great Gift Ranking."""

import polars as pl

import aosql


def solve():
    """Solve the puzzle."""
    return (
        pl.scan_parquet(aosql.table("gift_requests"))
        .with_columns(total_requests=pl.col("gift_id").count())
        .group_by("gift_id")
        .len()
        .with_columns(
            rank=pl.col("len").rank("min", descending=False),
            dense_rank=pl.col("len").rank("dense", descending=True),
        )
        .with_columns(percentiles=(pl.col("rank") / (pl.col("rank").count())))
        .filter(pl.col("dense_rank") == 2)
        .join(pl.scan_parquet(aosql.table("gifts")), on="gift_id")
        .sort(by="gift_name")
        .select(pl.col("gift_name"), pl.col("percentiles"))
        .head(1)
    ).collect()


if __name__ == "__main__":
    print(aosql.output(solve()))
