suite("test_explain_tpch_sf_1_q14", "tpch_sf1") {
    explain {
        sql """
                SELECT 100.00 * sum(CASE
                                    WHEN p_type LIKE 'PROMO%'
                                      THEN l_extendedprice * (1 - l_discount)
                                    ELSE 0
                                    END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
                FROM
                  lineitem,
                  part
                WHERE
                  l_partkey = p_partkey
                  AND l_shipdate >= DATE '1995-09-01'
                  AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH
            """
        check {
            explainStr ->
                explainStr.contains("runtime filters: RF000[in_or_bloom] -> `l_partkey`") &&
                        explainStr.contains("runtime filters: RF000[in_or_bloom] <- `p_partkey`") &&
                        explainStr.contains("PREDICATES: `l_shipdate` >= '1995-09-01 00:00:00', `l_shipdate` < '1995-10-01 00:00:00'")

        }
    }
}