-- Dedicated fixture for Doris Lance predicate-pushdown regression tests.
-- Two INSERT statements create multiple Lance versions/fragments. The scalar
-- columns cover every currently supported pushdown type. Each type contains
-- NULLs, boundary values, signed values or duplicates for operator coverage.
CREATE NAMESPACE IF NOT EXISTS lance.doris;

DROP TABLE IF EXISTS lance.doris.predicate_pushdown;

CREATE TABLE lance.doris.predicate_pushdown (
    row_id BIGINT NOT NULL,
    bool_value BOOLEAN,
    int8_value TINYINT,
    int16_value SMALLINT,
    int32_value INT,
    int64_value BIGINT,
    float32_value FLOAT,
    float64_value DOUBLE,
    decimal128_value DECIMAL(18, 2),
    utf8_value STRING,
    date32_value DATE,
    array_value ARRAY<STRING>
) USING lance;

INSERT INTO lance.doris.predicate_pushdown VALUES
    (1, NULL, CAST(NULL AS TINYINT), CAST(NULL AS SMALLINT), NULL,
     CAST(NULL AS BIGINT), CAST(NULL AS FLOAT), CAST(NULL AS DOUBLE),
     CAST(NULL AS DECIMAL(18, 2)), NULL, NULL, NULL),
    (2, false, CAST(-128 AS TINYINT), CAST(-32768 AS SMALLINT), -2147483648,
     CAST(-9223372036854775807 AS BIGINT), CAST(-100.5 AS FLOAT), CAST(-100.5 AS DOUBLE),
     CAST(-100.50 AS DECIMAL(18, 2)), 'minimum', DATE '1969-12-31', array('negative')),
    (3, false, CAST(-100 AS TINYINT), CAST(-100 AS SMALLINT), -100,
     CAST(-100 AS BIGINT), CAST(-100.0 AS FLOAT), CAST(-100.0 AS DOUBLE),
     CAST(-100.00 AS DECIMAL(18, 2)), 'negative', DATE '1970-01-01', array('negative')),
    (4, false, CAST(-1 AS TINYINT), CAST(-1 AS SMALLINT), -1,
     CAST(-1 AS BIGINT), CAST(-1.0 AS FLOAT), CAST(-1.0 AS DOUBLE),
     CAST(-1.00 AS DECIMAL(18, 2)), 'minus-one', DATE '2023-12-31', array('negative')),
    (5, true, CAST(0 AS TINYINT), CAST(0 AS SMALLINT), 0,
     CAST(0 AS BIGINT), CAST(0.0 AS FLOAT), CAST(0.0 AS DOUBLE),
     CAST(0.00 AS DECIMAL(18, 2)), '', DATE '2024-01-01', array()),
    (6, true, CAST(1 AS TINYINT), CAST(1 AS SMALLINT), 1,
     CAST(1 AS BIGINT), CAST(1.0 AS FLOAT), CAST(1.0 AS DOUBLE),
     CAST(1.00 AS DECIMAL(18, 2)), 'one', DATE '2024-01-02', array('positive'));

INSERT INTO lance.doris.predicate_pushdown VALUES
    (7, true, CAST(10 AS TINYINT), CAST(10 AS SMALLINT), 10,
     CAST(10 AS BIGINT), CAST(10.0 AS FLOAT), CAST(10.0 AS DOUBLE),
     CAST(10.00 AS DECIMAL(18, 2)), 'ten-a', DATE '2024-01-10', array('positive', 'lance')),
    (8, false, CAST(10 AS TINYINT), CAST(10 AS SMALLINT), 10,
     CAST(10 AS BIGINT), CAST(10.0 AS FLOAT), CAST(10.0 AS DOUBLE),
     CAST(10.00 AS DECIMAL(18, 2)), 'ten-b', DATE '2024-01-10', array('duplicate')),
    (9, true, CAST(100 AS TINYINT), CAST(100 AS SMALLINT), 100,
     CAST(100 AS BIGINT), CAST(100.0 AS FLOAT), CAST(100.0 AS DOUBLE),
     CAST(100.00 AS DECIMAL(18, 2)), 'hundred', DATE '2024-04-10', array('positive')),
    (10, true, CAST(127 AS TINYINT), CAST(32767 AS SMALLINT), 2147483647,
     CAST(9223372036854775807 AS BIGINT), CAST(1000.5 AS FLOAT), CAST(1000.5 AS DOUBLE),
     CAST(9999999999999999.99 AS DECIMAL(18, 2)), 'maximum', DATE '2024-12-31', array('positive')),
    (11, NULL, CAST(NULL AS TINYINT), CAST(NULL AS SMALLINT), NULL,
     CAST(NULL AS BIGINT), CAST(NULL AS FLOAT), CAST(NULL AS DOUBLE),
     CAST(NULL AS DECIMAL(18, 2)), NULL, NULL, NULL);

SELECT * FROM lance.doris.predicate_pushdown ORDER BY row_id;
