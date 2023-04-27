-- database: presto; groups: limit; tables: nation
SELECT COUNT(*) FROM
    (SELECT * FROM tpch_tiny_nation LIMIT 0) foo
