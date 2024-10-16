import logging
import tomli


with open("config.toml", "rb") as f:
    config = tomli.load(f)

logger = logging.getLogger(config["app"]["name"])


def convert_to_dicts(variable_list):
    return [
        {
            "table_id": item.table_id,
            "table_label": item.description,
            "universe": item.universe,
            "variable_id": item.id,
            "parent_id": item.parent_id,
            "variable_label": item.label,
            "children": None,
        }
        for item in variable_list
    ]


def nest_variables(variables, parent_id=None):
    """
    This will only work on small data, but it's n^2 in its current form.
    """
    tree = []

    for variable in variables:
        if variable["parent_id"] == parent_id:
            variable["children"] = nest_variables(
                variables, parent_id=variable["variable_id"]
            )

            tree.append(variable)

    return tree
