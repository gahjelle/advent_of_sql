"""Advent of SQL, day 19, 2024: Performance Review Season."""

import polars as pl

import aosql


def solve():
    """Solve the puzzle."""
    get_bonus = pl.col("last") > pl.col("last").mean()
    return (
        pl.scan_parquet(aosql.table("employees"))
        .select(
            pl.col("salary"),
            bonus=pl.col("salary") * 0.15,
            last=pl.col("year_end_performance_scores").list.last(),
        )
        .with_columns(total=pl.col("salary") + get_bonus * pl.col("bonus"))
        .select(pl.col("total").sum())
    ).collect()


if __name__ == "__main__":
    print(aosql.output(solve()))
