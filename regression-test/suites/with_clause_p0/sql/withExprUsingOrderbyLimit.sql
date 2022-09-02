-- database: presto; groups: with_clause; tables: nation; queryType: SELECT
WITH ordered AS (select nationkey a, regionkey b, name c from tpch_tiny_nation order by 1,2 limit 10)
select * from  ordered order by 1,2 limit 5
