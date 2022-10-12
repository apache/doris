select orderkey, suppkey, quantity,
round(sum(quantity) over (partition by suppkey order by orderkey rows between 2 following and unbounded following), 5) total_quantity
from tpch_tiny_lineitem where partkey = 272 order by suppkey, orderkey
