"""Advent of SQL, day #, 2024: <NAME>."""

from datetime import datetime

import polars as pl

import aosql


def solve():
    """Solve the puzzle.

    .pivot() isn't supported on lazy frames. Could do a manual .group_by() instead.
    """
    return (
        pl.read_parquet(aosql.table("drinks"))
        .pivot(on="drink_name", index="date", aggregate_function=pl.element().sum())
        .select(
            pl.col("date"),
            hot_cocoa=pl.col("quantity_Hot Cocoa"),
            peppermint_schnapps=pl.col("quantity_Peppermint Schnapps"),
            eggnog=pl.col("quantity_Eggnog"),
        )
        .filter(
            (pl.col("hot_cocoa") == 38)
            & (pl.col("peppermint_schnapps") == 298)
            & (pl.col("eggnog") == 198)
        )
        .select(pl.col("date"))
    )


if __name__ == "__main__":
    print(aosql.output(solve()))
