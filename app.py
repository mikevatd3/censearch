from flask import Flask, request, render_template
from sqlalchemy import text

from censearch.connection import db_engine
from censearch.nest_variables import nest_variables, convert_to_dicts


app = Flask(__name__, template_folder="./censearch/templates")


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
    """
    We're prioritizing 3 search strategies:

    1. Search table descriptions
        - Returns only the table id, table description, and universe
          with the match in the description highlighted.

    2. Search variable descriptions
        - Returns only the table id, table description, variable
          description and universe with the match in the description
          highlighted.

    3. TODO: Search table names
        - This is the only sub-word level search possibility

    4. TODO: Search variable names
        - Oops, no, this is sub-word seach as well

    Tweaks:
    - Somehow represent the more often used tables higher in the
      index.
        - This might require some manual annotating.
    - The split between table-level hits and var level hits is good,
      but it would be better if you could match terms on either level.
    - Include / how to include nested type variables -- like if you want to find
      females involved in fishing, hunting and trapping -- how do you get to the variable level.
    - Collapse racial iterations from search -- link from page.
    - Provide semantic suggestions from the table detail page
        - semantic similarity on all variable columns and fields.
    """
    q = request.args.get("q")

    stmt = text(
        """
        WITH table_highlights AS (
            SELECT 1 AS lev,
                   t.id,
                   ts_headline(t.description, q, 'StartSel="<mark>", StopSel="</mark>"') AS highlighted_text,
                   t.universe,
                   '' as parent_id,
                   '' as parent_label,
                   ts_rank(to_tsvector('english', t.id || ' ' || t.description), q) AS rnk
            FROM censearch.acs_tables t
            CROSS JOIN to_tsquery('english', :q) AS q
            WHERE (to_tsvector('english', t.id || ' ' || t.description) @@ q)
            ORDER BY ts_rank(to_tsvector('english', t.id || ' ' || t.description), q) / LENGTH(t.description) DESC
        ),
             var_highlights AS (
            SELECT DISTINCT ON (v.table_id)
                   2 as lev,
                   t.id,
                   t.description || E'\n' || v.id || ': ' || ts_headline(v.label, q, 'StartSel="<mark>", StopSel="</mark>"') AS highlighted_text,
                   t.universe,
                   vb.id as parent_id,
                   vb.label as parent_label,
                   ts_rank(to_tsvector('english', v.id || ' ' || v.label), q) AS rnk
            FROM censearch.acs_tables t
            JOIN censearch.acs_variables v
                ON v.table_id = t.id
            LEFT JOIN censearch.acs_variables vb
                ON v.parent_id = vb.id
            CROSS JOIN to_tsquery('english', :q) AS q
            WHERE (to_tsvector('english', v.id || ' ' || v.label) @@ q)
            ORDER BY v.table_id, ts_rank(to_tsvector('english', v.id || ' ' || v.label), q) / LENGTH(v.label) DESC
        )
        SELECT DISTINCT ON (lev, id) *
        FROM table_highlights
        UNION
        SELECT *
        FROM var_highlights
        ORDER BY lev;
    """
    )

    with db_engine.connect() as db:
        rows = db.execute(stmt, {"q": " & ".join(q.split())})  # type: ignore
        results = rows.fetchall()

    if not results:
        return render_template("no_results.html", query=q)

    hits = [
        {
            "id": row.id,
            "title": row.highlighted_text.split("\n")[0],
            "parent_id": row.parent_id,
            "parent_label": row.parent_label,
            "variable": row.highlighted_text.split("\n")[1]
            if (len(row.highlighted_text.split("\n")) > 1)
            else "",
            "universe": row.universe,
        }
        for row in results
    ]

    return render_template("search_results.html", results=hits)


if __name__ == "__main__":
    app.run(debug=True)
