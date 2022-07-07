suite("test_explain_tpch_sf_1_q16", "tpch_sf1") {
    explain {
        sql """

                SELECT
                  p_brand,
                  p_type,
                  p_size,
                  count(DISTINCT ps_suppkey) AS supplier_cnt
                FROM
                  partsupp,
                  part
                WHERE
                  p_partkey = ps_partkey
                  AND p_brand <> 'Brand#45'
                  AND p_type NOT LIKE 'MEDIUM POLISHED%'
                  AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
                  AND ps_suppkey NOT IN (
                    SELECT s_suppkey
                    FROM
                      supplier
                    WHERE
                      s_comment LIKE '%Customer%Complaints%'
                  )
                GROUP BY
                  p_brand,
                  p_type,
                  p_size
                ORDER BY
                  supplier_cnt DESC,
                  p_brand,
                  p_type,
                  p_size
                
            """
        check {
            explainStr -> {
                explainStr.contains("PREDICATES: `p_brand` != 'Brand#45', NOT `p_type` LIKE 'MEDIUM POLISHED%', `p_size` IN (49, 14, 23, 45, 19, 3, 36, 9)") &&
                        explainStr.contains("PREDICATES: `s_comment` LIKE '%Customer%Complaints%'") &&
                        explainStr.contains("runtime filters: RF000[in_or_bloom] -> `ps_partkey`") &&
                        explainStr.contains("runtime filters: RF000[in_or_bloom] <- `p_partkey`") &&
                        explainStr.contains("output slot ids: 3 4 5 6") &&
                        explainStr.contains("hash output slot ids: 3 4 5 6") &&
                        explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" +
                                "  |  equal join conjunct: `ps_partkey` = `p_partkey`") &&
                        explainStr.contains("join op: LEFT ANTI JOIN(BROADCAST)[Tables are not in the same group]\n" +
                                "  |  equal join conjunct: `ps_suppkey` = `s_suppkey`") &&
                        explainStr.contains("VTOP-N\n" +
                                "  |  order by: <slot 17> <slot 16> count(<slot 12> `ps_suppkey`) DESC, <slot 18> <slot 13> <slot 9> `p_brand` ASC, <slot 19> <slot 14> <slot 10> `p_type` ASC, <slot 20> <slot 15> <slot 11> `p_size` ASC")
            }
        }
    }
}