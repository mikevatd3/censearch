CREATE TABLE table_search_index (
    id SERIAL PRIMARY KEY,
    table_id VARCHAR(10),
    cat_vec TS_VECTOR,
    desc_vec TS_VECTOR -- Make this a ts_vector
);

-- search with cat_vec, then with desc_vec
