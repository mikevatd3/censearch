from pathlib import Path
import datetime
import pandas as pd
from sqlalchemy import text
from censearch.connection import db_engine


def main():
    today = datetime.date.today().strftime("%Y-%m-%d")
    with db_engine.connect() as db:
        for table in ["acs_tables", "acs_variables", "category_aliases"]:
            stmt = text(
                f"""
                select *
                from {table};
                """
            )

            rows = pd.read_sql(stmt, db)
            rows.to_csv(
                Path.cwd() / "backups" / "db" / f"{table}_bu_{today}.csv"
            )


if __name__ == "__main__":
    main()
