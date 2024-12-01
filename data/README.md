# Advent of SQL - Data

The data needed for the Advent of SQL challenges are made available in each challenge on [adventofsql.com](https://adventofsql.com/).

You should store the SQL-scripts inside a `2024/` directory. You can then run [dump_sql_to_parquet.py](dump_sql_to_parquet.py) to dump the database tables into Parquet files:

```console
$ python dump_sql_to_parquet.py 2024/advent_of_sql_day_1.sql
```

This will execute the SQL script inside your Postgresql database. Then, it will dump each table just created into a separate Parquet-file that you can use, for example to solve the challenges with Polars and Python.

Note that you need a Postgresql database for the script to work. Additionally, you need to set the following environment variables in place:

- `POSTGRES_USER`: For example, `postgres`
- `POSTGRES_PASSWORD`: For example, `very_merry_secret`
- `POSTGRES_SERVER`: For example, `localhost`
- `POSTGRES_PORT`: For example, `5432`
