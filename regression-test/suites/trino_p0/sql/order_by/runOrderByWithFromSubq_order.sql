select n_nationkey, n_regionkey, n_name from (select n_regionkey, n_nationkey, n_name from nation where n_nationkey < 20 order by 2 desc limit 5) t order by 2, 1 asc
