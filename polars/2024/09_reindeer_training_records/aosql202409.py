"""Advent of SQL, day 9, 2024: Reindeer Training Records."""

import polars as pl

import aosql


def solve():
    """Solve the puzzle."""
    return (
        pl.scan_parquet(aosql.table("training_sessions"))
        .join(
            pl.scan_parquet(aosql.table("reindeers"))
            .rename({"reindeer_name": "name"})
            .filter(pl.col("name") != "Rudolph"),
            on="reindeer_id",
        )
        .group_by("name", "exercise_name")
        .agg(speed=pl.col("speed_record").mean())
        .group_by("name")
        .agg(pl.col("speed").max())
        .sort(by="speed", descending=True)
        .head(3)
        .select(
            pl.col("name"),
            pl.col("speed").map_elements(
                lambda speed: f"{speed:.2f}", return_dtype=pl.String
            ),
        )
    ).collect()


if __name__ == "__main__":
    print(aosql.output(solve()))
