from dataclasses import dataclass
from pathlib import Path
import logging
import json

import pandas as pd
import tomli
from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.validation import Validator

from loader import build_workflow, StopThePresses, LoadFileType

from censearch.connection import db_engine
from censearch.app_logger import setup_logging

with open("config.toml", "rb") as f:
    config = tomli.load(f)


logger = logging.getLogger(config["app"]["name"])


def preload(filepath):
    """
    In the variable pre-load we need to handle the parent-child relationship
    of the variables in each table (using adjacency list).

    'variable' - the variable name or id or -- the thing that starts with B or C and the group/table name
    'label' - The thing with the !!
    'concept' - A longer form description of the table -- (can remove)
    'predicateType' - The datatype of the variable (int, float, str)
    'group' - The table name
    'limit' - Something to do with the API, but they are all zeros (remove)
    'attributes' - The extras that are available on the variable like MOE, and annotations (maybe remove?)
    """

    with open(filepath) as f:
        vars = json.load(f)

    result = [
        {"variable": variable, **fields}
        for variable, fields in vars["variables"].items()
        if fields["label"].split("!!")[0] == "Estimate"
    ]

    @dataclass
    class Variable:
        variable_id: str
        label: tuple[str, ...]
        predicate_type: str
        depth: int
        group: str
        parent: str | None = None

        @property
        def name(self):
            return (self.group, *self.label)

        @property
        def parent_name(self):
            return (self.group, *self.label[:-1])

        @property
        def short_label(self):
            return self.label[-1].strip(":")

    variables = sorted(
        [
            Variable(
                item["variable"].strip("E"),
                tuple(item["label"].split("!!")),
                item["predicateType"],
                len(item["label"].split("!!")),
                item["group"],
            )
            for item in result
        ],
        key=lambda var: var.depth,
    )

    name_to_varid = {var.name: var.variable_id for var in variables}
    
    good_stack = []
    error_stack = []
    """
    Here we want to deal with the fact there there are psuedo-variables
    that have children, but don't exist as a row in the table.

    For example, ('B19081', 'Estimate', 'Quintile Means', 'Fourth Quintile') exists but
    ('B19081', 'Estimate', 'Quintile Means') exists but doesn't. However, ('B19081', 'Estimate')
    might exist (it doesn't) so you need to combine and check again.

    You want to combine 'Quintile Means' and 'Fourth Quintile' to 'Quintile Means: Fourth Quintile'.
    """

    for var in variables:
        var.parent = name_to_varid.get(var.parent_name)
        if (
            (var.parent is None) 
            and (len(var.parent_name) > 2)
        ):
            error_stack.append(var)
        else:
            good_stack.append(var)

    while error_stack:
        var = error_stack.pop()
        *residual, a, b = var.label
        var.label = (*residual, f"{a} {b}")

        var.parent = name_to_varid.get(var.parent_name)

        if (
            (var.parent is None) 
            and (len(var.parent_name) > 2)
        ):
            error_stack.append(var)
        else:
            good_stack.append(var)
    
    return pd.DataFrame(
        [
            {
                "id": var.variable_id,
                "parent_id": var.parent,
                "table_id": var.group,
                "label": var.short_label,
                "data_type": var.predicate_type,
            }
            for var in variables
        ]
    ).set_index("id")


def cleanup_variables(variables):
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
    variables["edition_type"] = prompt(
        "Is this file for acs1 or acs5? ",
        completer=completer,
        validator=validator,
    )

    return variables


if __name__ == "__main__":
    setup_logging()

    run_workflow = build_workflow(
        config,
        "acs_variables",
        Path.cwd(),
        cleanup_variables,
        db_engine,
        preload=preload,
        filetype=LoadFileType.JSON,
        mute_metadata=True
    )

    run_workflow()
