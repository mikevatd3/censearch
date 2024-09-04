from dataclasses import dataclass
from pathlib import Path
import logging
import json

import pandas as pd
import tomli
from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.validation import Validator

from loader import build_workflow, LoadFileType

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

    return (
        pd.DataFrame(groups["groups"])
        .drop("variables", axis=1)
        .rename(columns={
            "name": "id",
            "universe ": "universe",
        })
        .set_index("id")
    )


def cleanup_tables(tables):
    """
    In this case, most the cleanup was done on the preload step. If there was more
    on-field manipulation that would be needed, this wouldn't be the case.

    We do need to capture from the user if the file is for acs1 or acs5 however.
    """

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
