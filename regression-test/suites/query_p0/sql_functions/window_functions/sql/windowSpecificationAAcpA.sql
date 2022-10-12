select * 
from 
    (select
        suppkey, orderkey, partkey,
        round(sum(quantity) over (partition by suppkey order by orderkey rows between unbounded preceding and current row), 3) sum_quantity_A,
        first_value(quantity) over (partition by suppkey order by orderkey rows between unbounded preceding and current row) first_value_quantity_A,
        round(sum(discount) over (partition by suppkey order by orderkey rows between unbounded preceding and current row), 3) sum_discount_A
    from tpch_tiny_lineitem 
    where (partkey = 272 or partkey = 273) and suppkey > 50
    ) as T
order by suppkey, orderkey
