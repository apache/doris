-- database: presto; groups: with_clause; tables: nation; queryType: SELECT
WITH ct AS (SELECT * FROM tpch_tiny_region) SELECT name FROM tpch_tiny_nation where nationkey = 0
