select orderkey, discount,
dense_rank() over (order by discount),
rank() over (order by discount range between unbounded preceding and current row)
from tpch_tiny_lineitem where partkey = 272
