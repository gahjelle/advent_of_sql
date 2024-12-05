"""Advent of SQL, day 5, 2024: Santa's Production Dashboard."""

import polars as pl

import aosql


def solve():
    """Solve the puzzle."""
    toys_produced = pl.col("toys_produced")
    production_change = toys_produced.diff()
    previous_day_production = toys_produced - production_change
    production_change_percentage = 100 * production_change / previous_day_production

    return (
        pl.scan_parquet(aosql.table("toy_production"))
        .sort(by="production_date")
        .with_columns(
            previous_day_production=previous_day_production,
            production_change=production_change,
            production_change_percentage=production_change_percentage,
        )
        .drop_nulls()
        .sort(by="production_change_percentage", descending=True)
        .head(1)
    ).collect()


if __name__ == "__main__":
    print(aosql.output(solve()))
