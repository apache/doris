-- Numeric primitives. Spark SQL does not expose Arrow unsigned integer types directly.
DROP TABLE IF EXISTS lance.doris.numeric_types;

CREATE TABLE lance.doris.numeric_types (
    row_id BIGINT NOT NULL,
    bool_value BOOLEAN,
    tinyint_value TINYINT,
    smallint_value SMALLINT,
    int_value INT,
    bigint_value BIGINT,
    float_value FLOAT,
    double_value DOUBLE
) USING lance;

INSERT INTO lance.doris.numeric_types VALUES
    (1, true, CAST(-128 AS TINYINT), CAST(-32768 AS SMALLINT), -2147483648,
     CAST(-9223372036854775807 AS BIGINT), CAST(-1.25 AS FLOAT), CAST(-1.25 AS DOUBLE)),
    (2, false, CAST(127 AS TINYINT), CAST(32767 AS SMALLINT), 2147483647,
     CAST(9223372036854775807 AS BIGINT), CAST(3.5 AS FLOAT), CAST(3.5 AS DOUBLE));

INSERT INTO lance.doris.numeric_types VALUES
    (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
