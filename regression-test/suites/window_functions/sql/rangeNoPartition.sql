select orderkey, discount, extendedprice,
min(extendedprice) over (order by discount range current row) min_extendedprice,
max(extendedprice) over (order by discount range current row) max_extendedprice
from tpch_tiny_lineitem where partkey = 272
