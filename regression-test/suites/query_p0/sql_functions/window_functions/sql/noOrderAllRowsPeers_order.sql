select orderkey, suppkey, discount,
rank() over (partition by suppkey)
from tpch_tiny_lineitem where partkey = 272
