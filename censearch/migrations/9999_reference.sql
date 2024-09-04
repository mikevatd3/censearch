WITH RECURSIVE tree AS (
    SELECT id, parent_id, name, 1 as level
    FROM records
    WHERE parent_id IS NULL
    UNION ALL
    SELECT r.id, r.parent_id, r.name, t.level + 1
    FROM records r
    JOIN tree t ON r.parent_id = t.id
)
SELECT * FROM tree ORDER BY level, id;
