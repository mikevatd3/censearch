# CENSEARCH

A census dataset searching tool. This version focuses on the ACS. It's an attempt to improve on the other search tools provided by the census or by census reporter. This relies on better text search queries, more intuitive display of results, and a more complete presentation of what datasets are available at what geographies, and for what publications.


## Plan 

- [x] Use ChatGPT to generate 200 example queries
- [x] Figure out returning results where there are some matching query but not all terms are present


## Thesauri (peh!)

You can use this feature, but it is applied globally to all queries which is not always wanted.

To create the document as a text file with a `.ths` extension. It should have the following format:

```
phrase to replace : replacement phrase
```

These are one to one on each line.

Place the `.ths` file in: `/usr/share/postgresql/16/tsearch_data/`


## TS Rewrite

The `ts_rewrite` function is a way to adjust queries 


## Query layers

- [ ] If the query starts with a 'b' or 'c' allow search for table codes directly with a 'like' clause.
- [ ] 'Short circuit' a few queries to encourage using the tables we use as a convention.
    - [ ] e.g. 'Race' should bring up first what is technically an ethnicity table 'B03002'
- [ ] Serve matches of category names (first three characters of table) with the entire category ordered
    - [ ] provide a strong thesaurus for these highest-level terms

- [ ] You want to default to the order suggested by the table codes, but reorder as matches are made deeper into the ACS hierarchy
HIERARCHY:
- short circuit
    - table keyword
        - table description
            - variable name (full variable hierarchy)



