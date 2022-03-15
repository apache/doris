-- database: presto; groups: limit; tables: nation
SELECT COUNT(*) FROM (SELECT * FROM tpch_tiny_nation LIMIT 2) AS foo LIMIT 5
