suite("test_explain_tpch_sf_1_q3", "tpch_sf1") {
    explain {
            sql """
		SELECT
		  l_orderkey,
		  sum(l_extendedprice * (1 - l_discount)) AS revenue,
		  o_orderdate,
		  o_shippriority
		FROM
		  customer,
		  orders,
		  lineitem
		WHERE
		  c_mktsegment = 'BUILDING'
		  AND c_custkey = o_custkey
		  AND l_orderkey = o_orderkey
		  AND o_orderdate < DATE '1995-03-15'
		  AND l_shipdate > DATE '1995-03-15'
		GROUP BY
		  l_orderkey,
		  o_orderdate,
		  o_shippriority
		ORDER BY
		  revenue DESC,
		  o_orderdate
		LIMIT 10

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 14> <slot 13> sum(`l_extendedprice` * (1 - `l_discount`)) DESC, <slot 15> <slot 11> `o_orderdate` ASC") && 
		explainStr.contains("VAGGREGATE (update finalize)\n" + 
				"  |  output: sum(`l_extendedprice` * (1 - `l_discount`))\n" + 
				"  |  group by: `l_orderkey`, `o_orderdate`, `o_shippriority`") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: `o_custkey` = `c_custkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `c_custkey`") && 
		explainStr.contains("output slot ids: 0 1 2 3 4 \n" + 
				"  |  hash output slot ids: 0 1 2 3 4 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_orderkey` = `o_orderkey`\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `o_orderkey`") && 
		explainStr.contains("output slot ids: 0 1 2 3 4 7 \n" + 
				"  |  hash output slot ids: 0 1 2 3 4 7 ") && 
		explainStr.contains("TABLE: lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `l_shipdate` > '1995-03-15 00:00:00'\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `l_orderkey`") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `c_mktsegment` = 'BUILDING'") && 
		explainStr.contains("TABLE: orders(orders), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `o_orderdate` < '1995-03-15 00:00:00'\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `o_custkey`") 
            
        }
    }
}