-- database: presto; groups: with_clause; tables: nation; queryType: SELECT
WITH nested AS (SELECT * FROM tpch_tiny_nation) SELECT count(*) FROM (select * FROM nested) as a
