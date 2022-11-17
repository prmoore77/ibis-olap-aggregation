from config import logger
import os
import duckdb
from pathlib import Path


DATABASE_FILENAME = "grocery_store.duckdb"

def get_data():

    data_dir = Path("data")
    database_file = data_dir / DATABASE_FILENAME

    if database_file.exists():
        os.remove(path=database_file)

    con = duckdb.connect(database=database_file.as_posix())

    with open(file="sql/create_grocery_store_database.sql", mode="r") as sql_file:
        query = sql_file.read()
        con.execute(query=query)


if __name__ == '__main__':
    get_data()
