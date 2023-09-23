-- database: presto; groups: with_clause; tables: nation; queryType: SELECT
WITH w1 AS (select min(nationkey) as x , max(regionkey) as y from tpch_tiny_nation),
w2 AS (select x, y from w1)
select count(*) count, regionkey from tpch_tiny_nation group by regionkey
union all
(select * from w2) order by regionkey, count
