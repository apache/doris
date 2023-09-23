SELECT /*+ SET_VAR(query_timeout = 600) */ ref_0.`availqty` AS c0,
                                           ref_0.`partkey` AS c1,
                                           coalesce(ref_0.`supplycost`, ref_0.`supplycost`) AS c2,
                                           ref_0.`availqty` AS c3,
                                           ref_0.`availqty` AS c4,
                                           ref_0.`partkey` AS c5,
                                           max(cast(ref_0.`availqty` AS int)) OVER (PARTITION BY ref_0.`supplycost`,
                                                                                                 ref_0.`comment`,
                                                                                                 ref_0.`partkey`
                                                                                    ORDER BY ref_0.`supplycost`) AS c6,
                                                                                   ref_0.`supplycost` AS c7,
                                                                                   ref_0.`supplycost` AS c8,
                                                                                   version() AS c9,
                                                                                   ref_0.`suppkey` AS c10
FROM regression_test_query_p0_sql_functions_string_functions.tpch_tiny_partsupp AS ref_0
WHERE TRUE
ORDER BY ref_0.`comment`
LIMIT 76
OFFSET 120
