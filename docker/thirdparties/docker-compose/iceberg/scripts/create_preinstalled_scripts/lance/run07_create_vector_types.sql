-- ARRAY<FLOAT> with this property is written by Spark as Arrow FixedSizeList, the Lance vector form.
DROP TABLE IF EXISTS lance.doris.vector_types;

CREATE TABLE lance.doris.vector_types (
    row_id BIGINT NOT NULL,
    label STRING,
    embedding ARRAY<FLOAT> NOT NULL
) USING lance
TBLPROPERTIES ('embedding.arrow.fixed-size-list.size' = '3');

INSERT INTO lance.doris.vector_types VALUES
    (1, 'origin', array(CAST(0.0 AS FLOAT), CAST(0.0 AS FLOAT), CAST(0.0 AS FLOAT))),
    (2, 'unit-x', array(CAST(1.0 AS FLOAT), CAST(0.0 AS FLOAT), CAST(0.0 AS FLOAT)));

INSERT INTO lance.doris.vector_types VALUES
    (3, 'mixed', array(CAST(-1.5 AS FLOAT), CAST(0.25 AS FLOAT), CAST(3.75 AS FLOAT)));
