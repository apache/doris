suite("test_explain_tpch_sf_1_q13", "tpch_sf1") {
    explain {
        sql """
            -- tables: customer
            SELECT
              c_count,
              count(*) AS custdist
            FROM (
                   SELECT
                     c_custkey,
                     count(o_orderkey) AS c_count
                   FROM
                     customer
                     LEFT OUTER JOIN orders ON
                                              c_custkey = o_custkey
                                              AND o_comment NOT LIKE '%special%requests%'
                   GROUP BY
                     c_custkey
                 ) AS c_orders
            GROUP BY
              c_count
            ORDER BY
              custdist DESC,
              c_count DESC
            """
        check {
            explainStr -> explainStr.contains("VTOP-N\n" +
                    "  |  order by: <slot 10> <slot 9> count(*) DESC, <slot 11> <slot 8> `c_count` DESC") &&
                    explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[Tables are not in the same group]\n" +
                            "  |  equal join conjunct: `c_custkey` = `o_custkey`") &&
                    explainStr.contains("TABLE: orders(orders), PREAGGREGATION: ON\n" +
                            "     PREDICATES: NOT `o_comment` LIKE '%special%requests%'")
        }
    }
}