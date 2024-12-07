"""Advent of SQL, day 7, 2024: Santa's Cartesian Elf Skill-Matching Program."""

import polars as pl

import aosql


def solve():
    """Solve the puzzle."""
    return (
        pl.scan_parquet(aosql.table("workshop_elves"))
        .group_by("primary_skill")
        .agg(
            elf_1_id=(
                pl.col("elf_id")
                .sort_by(by=["years_experience", "elf_id"], descending=[True, False])
                .first()
            ),
            elf_2_id=(
                pl.col("elf_id").sort_by(by=["years_experience", "elf_id"]).first()
            ),
        )
        .sort(by="primary_skill")
        .select(
            pl.col("elf_1_id"), pl.col("elf_2_id"), shared_skill=pl.col("primary_skill")
        )
        .head(3)
    ).collect()


if __name__ == "__main__":
    print(aosql.output(solve()))
