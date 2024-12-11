"""Advent of SQL, day 11, 2024: The Christmas Tree Famine."""

import polars as pl

import aosql


def solve():
    """Solve the puzzle."""
    seasons = {"Spring": 0, "Summer": 1, "Fall": 2, "Winter": 3}
    return (
        pl.scan_parquet(aosql.table("treeharvests"))
        .with_columns(
            harvest_num=(
                pl.col("harvest_year") * 4
                + pl.col("season").replace_strict(seasons, return_dtype=pl.Int16)
            )
        )
        .sort(by=["harvest_num"])
        .group_by("field_name")
        .agg(num_trees=pl.col("trees_harvested").rolling_mean(3))
        .select(pl.col("num_trees").list.max())
        .sort(by="num_trees", descending=True)
        .head(1)
        .select(
            pl.col("num_trees").map_elements(
                lambda num_trees: f"{num_trees:.2f}", return_dtype=pl.String
            )
        )
    ).collect()


if __name__ == "__main__":
    print(aosql.output(solve()))
