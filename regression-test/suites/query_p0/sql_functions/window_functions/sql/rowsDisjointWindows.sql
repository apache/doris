select orderkey, suppkey,
quantity,
round(sum(quantity) over (partition by suppkey order by orderkey rows between unbounded preceding and 1 preceding), 5) total_quantity,
extendedprice,
round(sum(extendedprice) over (partition by suppkey order by orderkey rows between current row and 1 following), 5)
total_extendedprice,
discount,
round(avg(discount) over (partition by suppkey order by orderkey rows between 3 following and unbounded following), 5)  avg_discount
from tpch_tiny_lineitem where partkey = 272
order by orderkey, suppkey
