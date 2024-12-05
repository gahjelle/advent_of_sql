"""Advent of SQL, day 3, 2024: The Greatest Christmas Dinner Ever"""

import re

import polars as pl

import aosql

FOOD_ITEMS = re.compile(r"<food_item_id>(\d+)</food_item_id>")


def solve():
    """Solve the puzzle."""
    xml = pl.col("menu_data").str
    return (
        pl.scan_parquet(aosql.table("christmas_menus"))
        .with_columns(
            num_people=pl.coalesce(
                xml.extract(r"<total_count>(\d+)</total_count>"),
                xml.extract(r"<total_guests>(\d+)</total_guests>"),
                xml.extract(r"<total_present>(\d+)</total_present>"),
            ).str.to_integer(),
            food_item=pl.col("menu_data").map_elements(
                list_food_items, return_dtype=pl.List(pl.String)
            ),
        )
        .filter(pl.col("num_people") >= 78)
        .explode(pl.col("food_item"))
        .group_by("food_item")
        .len()
        .sort(by="len", descending=True)
        .select(pl.col("food_item"))
        .head(1)
    ).collect()


def list_food_items(xml):
    """Use Python regex to extract all food items.

    Ideally, we'd use Polars' .extract_all() for this, but how to make it return
    groups instead of the full match?
    """
    return FOOD_ITEMS.findall(xml)


if __name__ == "__main__":
    print(aosql.output(solve()))
