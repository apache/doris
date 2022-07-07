suite("test_explain_tpch_sf_1_q4", "tpch_sf1") {
    explain {
            sql """
		SELECT
		  o_orderpriority,
		  count(*) AS order_count
		FROM orders
		WHERE
		  o_orderdate >= DATE '1993-07-01'
		  AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
		AND EXISTS (
		SELECT *
		FROM lineitem
		WHERE
		l_orderkey = o_orderkey
		AND l_commitdate < l_receiptdate
		)
		GROUP BY
		o_orderpriority
		ORDER BY
		o_orderpriority

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 38> <slot 36> `o_orderpriority` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 37> count(*))\n" + 
				"  |  group by: <slot 36> `o_orderpriority`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: `o_orderpriority`") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `o_orderkey` = `l_orderkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `l_orderkey`") && 
		explainStr.contains("output slot ids: 34 \n" + 
				"  |  hash output slot ids: 34 ") && 
		explainStr.contains("TABLE: orders(orders), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `o_orderdate` >= '1993-07-01 00:00:00', `o_orderdate` < '1993-10-01 00:00:00'\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `o_orderkey`") && 
		explainStr.contains("TABLE: lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `l_commitdate` < `l_receiptdate`") 
            
        }
    }
}