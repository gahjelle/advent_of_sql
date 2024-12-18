"""Advent of SQL, day 14, 2024: Where is Santa's Green Suit?"""

import polars as pl

import aosql


def solve():
    """Solve the puzzle."""
    return (
        (
            pl.scan_parquet(aosql.table("santarecords"))
            .with_columns(pl.col("cleaning_receipts").str.json_decode())
            .explode(pl.col("cleaning_receipts"))
            .unnest("cleaning_receipts")
        )
        .collect()  # Why do we need to collect before filter()? Struct doesn't have schema?
        .filter((pl.col("color") == "green") & (pl.col("garment") == "suit"))
        .sort(by="drop_off", descending=True)
        .select(pl.col("drop_off"))
        .head(1)
    )


if __name__ == "__main__":
    print(aosql.output(solve()))
