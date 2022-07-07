suite("test_explain_tpch_sf_1_q17", "tpch_sf1") {
    explain {
        sql """
                SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
                FROM
                  lineitem,
                  part
                WHERE
                  p_partkey = l_partkey
                  AND p_brand = 'Brand#23'
                  AND p_container = 'MED BOX'
                  AND l_quantity < (
                    SELECT 0.2 * avg(l_quantity)
                    FROM
                      lineitem
                    WHERE
                      l_partkey = p_partkey
                  )
            """
        check {
            explainStr -> {
                explainStr.contains("PREDICATES: `p_brand` = 'Brand#23', `p_container` = 'MED BOX'") &&
                explainStr.contains("runtime filters: RF001[in_or_bloom] -> `l_partkey`") &&
                explainStr.contains("output slot ids: 6 8 7") &&
                explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" +
                        "  |  equal join conjunct: `l_partkey` = `p_partkey`\n" +
                        "  |  runtime filters: RF001[in_or_bloom] <- `p_partkey`") &&
                explainStr.contains("output slot ids: 8 \n" +
                        "  |  hash output slot ids: 8 6 3") &&
                explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" +
                        "  |  equal join conjunct: `p_partkey` = <slot 2> `l_partkey`\n" +
                        "  |  other join predicates: `l_quantity` < 0.2 * <slot 3> avg(`l_quantity`)\n" +
                        "  |  runtime filters: RF000[in_or_bloom] <- <slot 2> `l_partkey`")
            }
        }
    }
}