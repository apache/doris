select /*+ SET_VAR(query_timeout = 600) */ ref_52.`key` as k, ref_54.`linestatus` as c0,  ref_54.`shipmode` as c1,    ref_54.`shipdate` as c2,    ref_54.`shipmode` as c3, ref_52.`value`  as c4 
from    nested_array_test_2_vectorized as ref_52  right join tpch_tiny_lineitem as ref_54  on (ref_52.`key` = ref_54.`linenumber` )
where ref_52.`value` is not NULL order by 1, 2, 3, 4, 5 limit 10;

select /*+ SET_VAR(query_timeout = 600) */ ref_52.`key`  as k, ref_54.`linestatus` as c0,  ref_54.`shipmode` as c1,    ref_54.`shipdate` as c2,    ref_54.`shipmode` as c3, ref_52.`value`  as c4 
from    nested_array_test_2_vectorized as ref_52  right join tpch_tiny_lineitem as ref_54  on (ref_52.`key` = ref_54.`linenumber` )
where ref_52.`value` is not NULL order by 1, 2, 3, 4, 5 limit 10;
