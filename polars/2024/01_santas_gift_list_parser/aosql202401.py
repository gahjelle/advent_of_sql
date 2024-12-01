"""Advent of SQL, day 1, 2024: Santa's Gift List Parser"""

import aosql

import polars as pl


def solve():
    """Solve the puzzle."""
    gift_complexity = pl.col("difficulty_to_make").cut(
        [1, 2], labels=["Simple Gift", "Moderate Gift", "Complex Gift"]
    )
    workshop_assignment = pl.col("category").replace_strict(
        {"outdoor": "Outside Workshop", "educational": "Learning Workshop"},
        default="General Workshop",
    )

    return (
        pl.scan_parquet(aosql.table("wish_lists"))
        .with_columns(
            color_count=(
                pl.col("wishes").str.json_path_match(r"$.colors").str.count_matches(",")
                + 1
            ),
            favorite_color=pl.col("wishes").str.json_path_match(r"$.colors[0]"),
            primary_wish=pl.col("wishes").str.json_path_match(r"$.first_choice"),
            backup_wish=pl.col("wishes").str.json_path_match(r"$.second_choice"),
        )
        .join(pl.scan_parquet(aosql.table("children")), on="child_id")
        .join(
            pl.scan_parquet(aosql.table("toy_catalogue")),
            left_on="primary_wish",
            right_on="toy_name",
        )
        .join(
            pl.scan_parquet(aosql.table("toy_catalogue")),
            left_on="backup_wish",
            right_on="toy_name",
        )
        .sort(by=["name", "child_id", "primary_wish"])
        .select(
            pl.col("name"),
            pl.col("primary_wish"),
            pl.col("backup_wish"),
            pl.col("favorite_color"),
            pl.col("color_count"),
            gift_complexity=gift_complexity,
            workshop_assignment=workshop_assignment,
        )
        .head(5)
    ).collect()


if __name__ == "__main__":
    print(aosql.output(solve()))
