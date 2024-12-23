"""Advent of SQL, day 21, 2024: Santa Chooses His Influencer."""

import polars as pl

import aosql


def solve():
    """Solve the puzzle."""
    return (
        pl.scan_parquet(aosql.table("sales"))
        .with_columns(
            year=pl.col("sale_date").dt.year(),
            quarter=pl.col("sale_date").dt.quarter(),
        )
        .group_by("year", "quarter")
        .agg(pl.col("amount").sum())
        .sort(by=["year", "quarter"])
        .with_columns(
            growth=(
                (pl.col("amount") - pl.col("amount").shift()) / pl.col("amount").shift()
            )
        )
        .drop_nulls()
        .sort(by="growth", descending=True)
        .head(1)
        .select(pl.col("year"), pl.col("quarter"))
    ).collect()


if __name__ == "__main__":
    print(aosql.output(solve()))
