SELECT COUNT(*) FROM (SELECT n1.regionkey, n1.nationkey FROM tpch_tiny_nation n1 JOIN tpch_tiny_nation n2 ON n1.regionkey = n2.regionkey LIMIT 5) foo
