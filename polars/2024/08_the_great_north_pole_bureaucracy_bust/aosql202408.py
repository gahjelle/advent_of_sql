"""Advent of SQL, day 8, 2024: The Great North Pole Bureaucracy Bust ."""

import polars as pl

import aosql


def find_line_of_command(staff, acc=None, join_level=1, stack_level=1, levels=None):
    """Recursively calculate managers of managers.

    https://stackoverflow.com/questions/76238414/recursively-lookup-value-with-polars
    """
    levels = {} if levels is None else levels
    tiers = (
        staff.with_columns(is_manager=pl.col("staff_id").is_in(pl.col("manager_1_id")))
        .sort(by="is_manager")
        .partition_by("is_manager", include_key=False, as_dict=True)
    )
    employees = tiers.get((False,))
    management = tiers.get((True,))
    if acc is None:
        acc = employees

    if management is None:
        if stack_level + 1 in levels.keys():
            return pl.concat([acc, levels[stack_level + 1]], how="diagonal")
        else:
            return acc

    # Walk up the management tree
    if stack_level + 1 not in levels.keys():
        levels[stack_level + 1] = find_line_of_command(
            management, stack_level=stack_level + 1
        )

    # Walk back down while concatenating mid-management levels
    acc = acc.join(
        management.rename({"manager_1_id": f"manager_{join_level+1}_id"}),
        left_on=f"manager_{join_level}_id",
        right_on="staff_id",
        how="left",
    )
    return find_line_of_command(
        management,
        acc=acc,
        stack_level=stack_level,
        join_level=join_level + 1,
        levels=levels,
    )


def solve():
    """Solve the puzzle."""
    staff = (
        pl.read_parquet(aosql.table("staff"))
        .rename({"manager_id": "manager_1_id"})
        .drop("staff_name")
        .fill_null(strategy="zero")
    )
    return (
        find_line_of_command(staff)
        .select(
            num_levels=(
                pl.concat_list(pl.col(r"^manager_\d+_id$")).list.drop_nulls().list.len()
            ),
        )
        .sort(by="num_levels", descending=True)
        .head(1)
    )


def non_polars():
    """Solve the puzzle without Polars"""
    data = pl.read_parquet(aosql.table("staff")).to_dict(as_series=False)
    managers = dict(zip(data["staff_id"], data["manager_id"]))

    lines_of_command = {}
    for person, manager in managers.items():
        line_of_command = [person]
        while manager is not None:
            if manager in lines_of_command:
                lines_of_command[line_of_command[0]] = (
                    line_of_command + lines_of_command[manager]
                )
                break
            else:
                person, manager = manager, managers[manager]
        else:
            lines_of_command[line_of_command[0]] = line_of_command
    max_loc = max(len(loc) for loc in lines_of_command.values())
    print(
        [
            (person, loc)
            for person, loc in lines_of_command.items()
            if len(loc) == max_loc
        ]
    )
    return max_loc


if __name__ == "__main__":
    print(aosql.output(solve()))
