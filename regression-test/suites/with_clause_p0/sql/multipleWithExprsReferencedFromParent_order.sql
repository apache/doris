-- database: presto; groups: with_clause; tables: nation,region; queryType: SELECT
WITH wnation AS (SELECT nationkey, regionkey FROM tpch_tiny_nation),
wregion AS (SELECT r_regionkey, r_name FROM tpch_tiny_region)
select n.nationkey, r.r_regionkey from wnation n join wregion r on n.regionkey = r.r_regionkey
where r.r_name = 'AFRICA'
