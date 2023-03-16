-- database: presto; groups: limit; tables: nation
SELECT COUNT(*), regionkey FROM tpch_tiny_nation GROUP BY regionkey
ORDER BY regionkey DESC
LIMIT 2
