from itertools import groupby
from flask import Flask, jsonify, request, render_template
from sqlalchemy import text

from censearch.connection import db_engine
from censearch.nest_variables import nest_variables, convert_to_dicts
from censearch.app_logger import setup_logging


setup_logging()

app = Flask(__name__, template_folder="./censearch/templates")

# Adding zip to jinja env because we iterate over var labels / ids
app.jinja_env.globals.update(zip=zip)

@app.route("/censearch/search-home")
def search_home():
    return render_template("search_home.html")


@app.route("/censearch/tables/<table_id>")
def table_detail_page(table_id):
    stmt = text(
        """
        SELECT t.id as table_id,
               t.description,
               t.universe,
               v.id,
               v.parent_id,
               v.label
        FROM censearch.acs_variables v
        JOIN censearch.acs_tables t
        ON v.table_id = t.id
        WHERE table_id = :table_id
        ORDER BY v.id;
    """
    )

    with db_engine.connect() as db:
        result = db.execute(stmt, {"table_id": table_id})
        rows = result.fetchall()

    nested_variables = nest_variables(convert_to_dicts(rows))
    head = nested_variables[0]

    table = {
        "table_id": head["table_id"],
        "table_label": head["table_label"],
        "universe": head["universe"],
    }

    return render_template(
        "table_detail.html", table=table, variables=nested_variables
    )


@app.route("/censearch/text-search")
def text_search():
    q = request.args.get("q")
    how = request.args.get("how", "html")

    stmt = text(
        """
        WITH params AS (
            SELECT ts_rewrite(
                websearch_to_tsquery(:q), 
                'SELECT expected_q, alias_q FROM censearch.category_aliases'
            ) AS prepped_q
        ),
             table_results AS (
            SELECT
               id as table_id,
               '' as variable_id,
               ts_headline(
                   'english', description, prepped_q, 
                   'MaxWords=200, StartSel="<mark>", StopSel="</mark>"'
               ) AS highlighted_table,
               '' as highlighted_variable,
               universe,
               ts_rank(
                   '{0.25, 0.5, 0.75, 1.0}',
                   setweight(to_tsvector(keyword), 'A')
                   || setweight(to_tsvector(unkeyed), 'C'),
                   params.prepped_q
               ) as rnk  
            FROM censearch.acs_tables, params
            WHERE (
                    to_tsvector(keyword) @@ params.prepped_q
                    OR to_tsvector(unkeyed) @@ params.prepped_q
                    OR id like :q || '%' -- Last resort
                )
                AND length(id) = 6
             ),
             variable_results AS (
            SELECT
               tab.id as table_id,
               var.id as variable_id,
               ts_headline(
                   'english', tab.description, prepped_q, 
                   'MaxWords=200, StartSel="<mark>", StopSel="</mark>"'
               ) AS highlighted_table,
               ts_headline(
                   'english', full_label, prepped_q, 
                   'MaxWords=200, StartSel="<mark>", StopSel="</mark>"'
               ) AS highlighted_variable,
               tab.universe,
               ts_rank(
                   '{0.25, 0.5, 0.75, 1.0}',
                   setweight(to_tsvector(tab.keyword), 'B')
                   || setweight(to_tsvector(tab.unkeyed), 'C')
                   || setweight(to_tsvector(full_label), 'D'),
                   params.prepped_q
               ) as rnk  
            FROM censearch.acs_variables AS var
            LEFT JOIN censearch.acs_tables AS tab
                ON tab.id = var.table_id
            CROSS JOIN params 
            WHERE to_tsvector(full_label) @@ params.prepped_q
                AND length(var.table_id) = 6
             )
        SELECT table_id, 
               variable_id, 
               highlighted_table, 
               highlighted_variable, 
               universe,
               rnk
        FROM (
            SELECT * 
            FROM table_results t
            UNION ALL
            SELECT *            
            FROM variable_results v
            WHERE NOT EXISTS (
                SELECT 1
                FROM table_results t
                WHERE t.table_id = v.table_id
            )
        ) AS everything
        ORDER BY rnk desc, table_id, variable_id
        LIMIT 10;
        """
    )

    with db_engine.connect() as db:
        rows = db.execute(stmt, {"q": q})  # type: ignore
        results = rows.fetchall()

    if not results or not q:
        return render_template("no_results.html", query=q)

    row_dicts = [
        {
            "table": row.highlighted_table,
            "table_id": row.table_id,
            "variable": row.highlighted_variable,
            "variable_id": row.variable_id,
            "universe": row.universe,
        }
        for row in results
    ]

    table_groups = groupby(row_dicts, key=lambda x: x["table_id"])

    hits = []
    for table in table_groups:
        """
        Loop over group results an capture mutliple hits from the same
        table and add variables as a list. If there is no variable data
        you can continue with a table-level result.
        """

        _, var_iter = table

        complete_table = None
        for row in var_iter:
            if not row["variable"]:
                # If you find a table-level result return it directly but
                # switch to list for a type match
                row["variable_id"] = []
                row["variable"] = []
                complete_table = row
                break

            if not complete_table:
                # If this is the first, row unpack evenything and switch
                # variable & variable_id to list
                complete_table = {**row}
                complete_table["variable_id"] = [row["variable_id"]]
                complete_table["variable"] = [row["variable"]]

            else:
                complete_table["variable_id"].append(row["variable_id"])
                complete_table["variable"].append(row["variable"])

        hits.append(complete_table)

    if how == "json":
        return jsonify(hits)

    return render_template("search_results.html", results=hits)


if __name__ == "__main__":
    app.run(debug=True)
