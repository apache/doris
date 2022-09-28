select * 
from (
    select orderkey, suppkey, quantity,
    round(sum(quantity) over (partition by suppkey order by orderkey rows unbounded preceding), 5) total_quantity
    from tpch_tiny_lineitem where partkey = 272
)as T
order by suppkey, orderkey
