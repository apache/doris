suite("test_explain_tpch_sf_1_q21", "tpch_sf1") {
    explain {
        sql """
            SELECT
              s_name,
              count(*) AS numwait
            FROM
              supplier,
              lineitem l1,
              orders,
              nation
            WHERE
              s_suppkey = l1.l_suppkey
              AND o_orderkey = l1.l_orderkey
              AND o_orderstatus = 'F'
              AND l1.l_receiptdate > l1.l_commitdate
              AND exists(
                SELECT *
                FROM
                  lineitem l2
                WHERE
                  l2.l_orderkey = l1.l_orderkey
                  AND l2.l_suppkey <> l1.l_suppkey
              )
              AND NOT exists(
                SELECT *
                FROM
                  lineitem l3
                WHERE
                  l3.l_orderkey = l1.l_orderkey
                  AND l3.l_suppkey <> l1.l_suppkey
                  AND l3.l_receiptdate > l3.l_commitdate
              )
              AND s_nationkey = n_nationkey
              AND n_name = 'SAUDI ARABIA'
            GROUP BY
              s_name
            ORDER BY
              numwait DESC,
              s_name
            LIMIT 100
            """
        check {
            explainStr -> {
                explainStr.contains("TABLE: supplier(supplier), PREAGGREGATION: ON\n" +
                        "     runtime filters: RF001[in_or_bloom] -> `s_nationkey`") &&
                explainStr.contains("TABLE: orders(orders), PREAGGREGATION: ON\n" +
                        "     PREDICATES: `o_orderstatus` = 'F'") &&
                explainStr.contains("TABLE: nation(nation), PREAGGREGATION: ON\n" +
                        "     PREDICATES: `n_name` = 'SAUDI ARABIA'") &&
                explainStr.contains("TABLE: lineitem(lineitem), PREAGGREGATION: ON\n" +
                        "     PREDICATES: `l1`.`l_receiptdate` > `l1`.`l_commitdate`\n" +
                        "     runtime filters: RF000[in_or_bloom] -> `l1`.`l_orderkey`, RF002[in_or_bloom] -> `l1`.`l_orderkey`, RF003[in_or_bloom] -> `l1`.`l_suppkey`") &&
                explainStr.contains("output slot ids: 34 35 70 76 \n" +
                        "  |  hash output slot ids: 34 35 70 76") &&
                explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" +
                        "  |  equal join conjunct: `l1`.`l_suppkey` = `s_suppkey`\n" +
                        "  |  runtime filters: RF003[in_or_bloom] <- `s_suppkey`") &&
                explainStr.contains("output slot ids: 34 35 70 76 \n" +
                        "  |  hash output slot ids: 34 35 70 76") &&
                explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" +
                        "  |  equal join conjunct: `l1`.`l_orderkey` = `o_orderkey`\n" +
                        "  |  runtime filters: RF002[in_or_bloom] <- `o_orderkey`") &&
                explainStr.contains("output slot ids: 34 35 70 \n" +
                        "  |  hash output slot ids: 34 35 70 ") &&
                explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" +
                        "  |  equal join conjunct: `s_nationkey` = `n_nationkey`\n" +
                        "  |  runtime filters: RF001[in_or_bloom] <- `n_nationkey`") &&
                explainStr.contains("join op: LEFT SEMI JOIN(COLOCATE[])[]\n" +
                        "  |  equal join conjunct: `l1`.`l_orderkey` = `l2`.`l_orderkey`\n" +
                        "  |  other join predicates: `l2`.`l_suppkey` != `l1`.`l_suppkey`\n" +
                        "  |  runtime filters: RF000[in_or_bloom] <- `l2`.`l_orderkey`") &&
                explainStr.contains("output slot ids: 34 35 70 \n" +
                        "  |  hash output slot ids: 34 35 70 1") &&
                explainStr.contains("TABLE: lineitem(lineitem), PREAGGREGATION: ON\n" +
                        "  |       PREDICATES: `l3`.`l_receiptdate` > `l3`.`l_commitdate`") &&
                explainStr.contains("output slot ids: 70 \n" +
                        "  |  hash output slot ids: 70 37 34") &&
                explainStr.contains("join op: LEFT ANTI JOIN(COLOCATE[])[]\n" +
                        "  |  equal join conjunct: `l1`.`l_orderkey` = `l3`.`l_orderkey`\n" +
                        "  |  other join predicates: `l3`.`l_suppkey` != `l1`.`l_suppkey`") &&
                explainStr.contains("VTOP-N\n" +
                        "  |  order by: <slot 81> <slot 80> count(*) DESC, <slot 82> <slot 79> `s_name` ASC")

            }
        }
    }
}