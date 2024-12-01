#!/usr/bin/env python
"""Test outputs of Advent of SQL puzzle solutions."""

# Standard library imports
import importlib
import pathlib

# Advent of SQL imports
import aosql

# Third party imports
import pytest
from codetiming import Timer

PUZZLE_DIR = pathlib.Path(__file__).parent
PUZZLE_PATHS = sorted(p.parent for p in PUZZLE_DIR.rglob("**/output.py.txt"))


class TimingsLog:
    """Logger that can write timings to file."""

    time_units = (
        ("m ⚫️", 60),
        ("s 🔴", 1),
        ("ms 🔵", 1e-3),
        ("μs ⚪️", 1e-6),
        ("ns ⚪️", 1e-9),
    )
    fmt_header = (
        "\n## {year}\n\n"
        "| Day | Puzzle | Polars | Runtime |\n"
        "|:---|:---|:---|---:|\n"
    )
    fmt_entry = "| {day} | {puzzle} | {link} | {total} |\n"

    def __init__(self, path):
        """Initialize logger."""
        self.path = path
        self.path.write_text("# Advent of SQL\n")
        self.current_year = 0

    def write_log(self, year, day, puzzle, link, total):
        """Write an entry in the log."""
        if year != self.current_year:
            self.current_year = year
            self.write_line(self.fmt_header.format(year=year))

        self.write_line(
            self.fmt_entry.format(
                day=day,
                puzzle=puzzle,
                link=link,
                total=self.prettytime(total),
            )
        )

    def write_line(self, line):
        """Write one line to the log file."""
        with self.path.open(mode="a") as fid:
            fid.write(line)

    @classmethod
    def prettytime(cls, seconds):
        """Pretty-print number of seconds."""
        for unit, threshold in cls.time_units:
            if seconds > threshold:
                return f"{seconds / threshold:.2f} {unit}"


TIMINGS_LOG = TimingsLog(PUZZLE_DIR / "timings.py.md")


@pytest.mark.parametrize(
    "puzzle_path", PUZZLE_PATHS, ids=[p.name for p in PUZZLE_PATHS]
)
def test_puzzle(puzzle_path):
    """Test one puzzle against the expected solution."""

    # Import puzzle
    *_, year_dir, puzzle = puzzle_path.parts
    day = puzzle[:2]
    year, *_ = year_dir.split("_")
    puzzle_mod = importlib.import_module(f"{year_dir}.{puzzle}.aosql{year}{day}")

    # Solve the puzzle
    solve = getattr(puzzle_mod, "solve")
    with Timer(logger=None) as timer:
        solution = aosql.output(solve())

    # Compare to expected output
    expected = (puzzle_path / "output.py.txt").read_text().rstrip()
    assert solution == expected

    # Log elapsed time
    puzzle_name = puzzle[3:].replace("_", " ").title()
    link = f"[aosql{year}{day}.py]({puzzle}/aosql{year}{day}.py)"
    TIMINGS_LOG.write_log(
        year=year, day=int(day), puzzle=puzzle_name, link=link, total=timer.last
    )
