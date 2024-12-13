"""Advent of SQL, day 13, 2024: Santa's Christmas Card List."""

import polars as pl

import aosql


def solve():
    """Solve the puzzle."""
    return (
        pl.scan_parquet(aosql.table("contact_list"))
        .explode("email_addresses")
        .with_columns(domain=pl.col("email_addresses").str.extract(r"@([.a-z]+)"))
        .group_by("domain")
        .len()
        .sort(by="len", descending=True)
        .select(pl.col("domain"))
        .head(1)
    ).collect()


if __name__ == "__main__":
    print(aosql.output(solve()))
