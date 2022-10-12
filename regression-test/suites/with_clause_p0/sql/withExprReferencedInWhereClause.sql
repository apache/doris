-- database: presto; groups: with_clause; tables: nation,region; queryType: SELECT
WITH wregion AS (select min(regionkey) from tpch_tiny_nation where name >= 'N')
select r_regionkey, r_name from tpch_tiny_region where r_regionkey IN (SELECT * FROM wregion)
