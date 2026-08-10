-- Dedicated fixture for Doris Lance vector_search() regression tests.
-- Two INSERT statements intentionally create multiple Lance fragments. The
-- companion create_vector_search_index.py builds the vector index because
-- lance-spark-bundle 0.4.0 does not expose vector index creation through SQL.
CREATE NAMESPACE IF NOT EXISTS lance.doris;

DROP TABLE IF EXISTS lance.doris.vector_search;

CREATE TABLE lance.doris.vector_search (
    row_id BIGINT NOT NULL,
    category STRING NOT NULL,
    label STRING NOT NULL,
    embedding ARRAY<FLOAT> NOT NULL
) USING lance
TBLPROPERTIES ('embedding.arrow.fixed-size-list.size' = '4');

-- row_id 1 is the origin. For query [0, 0, 0, 0], the exact squared L2
-- distance of row_id n is 30 * (n - 1)^2.
INSERT INTO lance.doris.vector_search
SELECT
    id + 1 AS row_id,
    CASE WHEN id % 2 = 0 THEN 'even' ELSE 'odd' END AS category,
    concat('item-', lpad(CAST(id + 1 AS STRING), 4, '0')) AS label,
    array(
        CAST(id AS FLOAT),
        CAST(id * 2 AS FLOAT),
        CAST(id * 3 AS FLOAT),
        CAST(id * 4 AS FLOAT)
    ) AS embedding
FROM range(0, 512);

INSERT INTO lance.doris.vector_search
SELECT
    id + 1 AS row_id,
    CASE WHEN id % 2 = 0 THEN 'even' ELSE 'odd' END AS category,
    concat('item-', lpad(CAST(id + 1 AS STRING), 4, '0')) AS label,
    array(
        CAST(id AS FLOAT),
        CAST(id * 2 AS FLOAT),
        CAST(id * 3 AS FLOAT),
        CAST(id * 4 AS FLOAT)
    ) AS embedding
FROM range(512, 1024);

SELECT count(*) AS row_count, min(row_id) AS min_id, max(row_id) AS max_id
FROM lance.doris.vector_search;
