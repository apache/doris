    select orderkey, discount, extendedprice,
    min(extendedprice) over (partition by discount order by discount) min_extendedprice,
    max(extendedprice) over (partition by discount order by discount) max_extendedprice
    from tpch_tiny_lineitem as T where partkey = 272
