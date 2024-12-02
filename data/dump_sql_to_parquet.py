import os
import sys
from pathlib import Path

import psycopg

import polars as pl

DB_NAME = "santa_workshop"
DB_URI = (
    f"postgresql://{os.environ["POSTGRES_USER"]}:{os.environ["POSTGRES_PASSWORD"]}"
    f"@{os.environ["POSTGRES_SERVER"]}:{os.environ["POSTGRES_PORT"]}/{DB_NAME}"
)


def main(file_paths):
    """Convert each of the given sql files."""
    for file_path in file_paths:
        update_database(file_path)
        dump_to_parquet(file_path)


def update_database(file_path):
    """Run the SQL script to update the database."""
    print(f"Executing {file_path.name}")
    with psycopg.connect(
        f"dbname={DB_NAME} user={os.environ["POSTGRES_USER"]}"
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(file_path.read_bytes())
            conn.commit()


def dump_to_parquet(file_path):
    """Dump each table in the SQL script to a Parquet file."""
    day = int(file_path.stem.split("_")[-1])
    year = file_path.parent.name
    data_dir = file_path.parent
    for line in file_path.read_text().split("\n"):
        if not line.startswith("CREATE TABLE "):
            continue

        table_name = line.split()[2].lower()
        out_path = data_dir / f"aosql{year}{day:02d}_{table_name}.parquet"

        print(f"Writing table {table_name} to {out_path}")
        (
            pl.read_database_uri(
                query=f"SELECT * FROM {table_name}", uri=DB_URI
            ).write_parquet(out_path)
        )


if __name__ == "__main__":
    main([Path(path) for path in sys.argv[1:]])
