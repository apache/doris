-- Nested containers cover NULL containers, empty containers, NULL children and nesting.
-- MAP columns are deliberately excluded: lance-spark-bundle 0.4.0 cannot encode Arrow MAP yet.
DROP TABLE IF EXISTS lance.doris.complex_types;

CREATE TABLE lance.doris.complex_types (
    row_id BIGINT NOT NULL,
    int_array ARRAY<INT>,
    profile STRUCT<city: STRING, level: INT>,
    visits ARRAY<STRUCT<page: STRING, duration_seconds: INT>>
) USING lance;

INSERT INTO lance.doris.complex_types VALUES
    (1, array(1, NULL, 3),
     named_struct('city', 'Beijing', 'level', 7),
     array(named_struct('page', 'home', 'duration_seconds', 5),
           named_struct('page', 'search', 'duration_seconds', NULL))),
    (2, array(), named_struct('city', '', 'level', 0), array());

INSERT INTO lance.doris.complex_types VALUES
    (3, NULL, NULL, NULL);
