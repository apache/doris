select * 
from 
    (select orderkey, suppkey,
        discount,
        lead(discount, 1, null) over (partition by suppkey order by orderkey desc) next_discount,
        extendedprice,
        lag(extendedprice, 1, null) over (partition by discount order by extendedprice) previous_extendedprice
      from tpch_tiny_lineitem 
      where partkey = 272) 
      as T
order by orderkey
