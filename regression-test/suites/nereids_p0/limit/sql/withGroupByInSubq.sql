-- database: presto; groups: limit; tables: partsupp
SELECT COUNT(*) FROM (
    SELECT suppkey, COUNT(*) FROM tpch_tiny_partsupp
    GROUP BY suppkey LIMIT 20) t1
