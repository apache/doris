select n_regionkey, count(*), sum(n_nationkey) from nation group by 1
