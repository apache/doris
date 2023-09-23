-- database: presto; groups: limit; tables: nation
SELECT nationkey from tpch_tiny_nation ORDER BY nationkey DESC LIMIT 5
