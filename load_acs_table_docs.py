from pathlib import Path
import logging
import json

import pandas as pd
import tomli
from nltk.stem import WordNetLemmatizer
from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.validation import Validator

from loader import build_workflow, LoadFileType, StopThePresses

from censearch.connection import db_engine
from censearch.app_logger import setup_logging

with open("config.toml", "rb") as f:
    config = tomli.load(f)


logger = logging.getLogger(config["app"]["name"])


def preload(filepath):
    """
    Luckily on the tables, we just need to read one level down into the json.
    Everything else is good to go.
    """

    with open(filepath) as f:
        groups = json.load(f)

    top_lev_labels = pd.read_csv(
        Path.cwd() / "raw" / "table_code_keywords.csv",
        dtype={"top_lev_code": "str", "keyword": "str"},
    )

    ltizer = WordNetLemmatizer()

    def filter_keys(row):
        logger.info(row)
        kw = ltizer.lemmatize(row["keyword"].lower())
        return " ".join(
            {
                ltizer.lemmatize(w.lower())
                for w in row["description"].split()
                if w != kw
            }
        )

    logger.info(top_lev_labels.columns)

    result = (
        pd.DataFrame(groups["groups"])
        .drop("variables", axis=1)
        .rename(
            columns={
                "name": "id",
                "universe ": "universe",
            }
        )
        .assign(
            top_lev_code=lambda df: df["id"].str.slice(1, 3),
        )
        .merge(top_lev_labels, on="top_lev_code")
        .assign(
            unkeyed=lambda df: df.apply(filter_keys, axis=1),
        )
        .drop("top_lev_code", axis=1)
        .set_index("id")
    )

    logger.info(result["unkeyed"].sample(20))

    return result


def cleanup_tables(tables):
    """
    In this case, most the cleanup was done on the preload step. If there
    was more on-field manipulation that would be needed, this wouldn't be
    the case.

    We do need to capture from the user if the file is for acs1 or acs5
    however.
    """

    logger.info(tables.columns)

    acceptable_editions = ["acs1", "acs5"]

    def validate(string):
        return string in acceptable_editions

    validator = Validator.from_callable(validate)

    completer = WordCompleter(acceptable_editions)
    tables["edition_type"] = prompt(
        "Is this file for acs1 or acs5? ",
        completer=completer,
        validator=validator,
    )

    return tables


if __name__ == "__main__":
    setup_logging()

    run_workflow = build_workflow(
        config,
        "acs_tables",
        Path.cwd(),
        cleanup_tables,
        db_engine,
        preload=preload,
        filetype=LoadFileType.JSON,
        mute_metadata=True,
    )

    run_workflow()
