from pathlib import Path
import pandas as pd
import tomli
from sqlalchemy import text

from censearch.connection import db_engine


with open("config.toml", "rb") as f:
    config = tomli.load(f)


def main():
    aliases = pd.read_csv(Path.cwd() / "raw" / "aliases.csv")
    aliases.to_sql(
        "category_aliases", db_engine, schema="censearch", if_exists="replace"
    )


if __name__ == "__main__":
    main()
