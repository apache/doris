SELECT n_regionkey, COUNT(null) FROM nation WHERE n_nationkey > 5 GROUP BY n_regionkey
