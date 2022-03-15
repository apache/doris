select orderkey, suppkey, extendedprice,
first_value(extendedprice) over (partition by suppkey order by extendedprice desc rows between unbounded preceding and unbounded following),
last_value(extendedprice) over (partition by suppkey order by extendedprice desc rows between unbounded preceding and unbounded following)
from tpch_tiny_lineitem where partkey = 272
