-- database: presto; groups: limit; tables: nation
SELECT foo.c, foo.regionkey FROM
    (SELECT regionkey, COUNT(*) AS c FROM tpch_tiny_nation
    GROUP BY regionkey ORDER BY regionkey LIMIT 2) foo order by 2,1
