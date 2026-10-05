-- database: presto; groups: with_clause; tables: nation; queryType: SELECT
WITH wnation AS (SELECT name, nationkey, regionkey FROM tpch_tiny_nation)
SELECT n1.name, n2.name FROM wnation n1 JOIN wnation n2
ON n1.nationkey=n2.regionkey
