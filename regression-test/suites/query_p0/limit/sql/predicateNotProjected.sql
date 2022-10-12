-- database: presto; groups: limit; tables: nation
SELECT nationkey FROM tpch_tiny_nation WHERE name < 'INDIA'
ORDER BY nationkey LIMIT 3
