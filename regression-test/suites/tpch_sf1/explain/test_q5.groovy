suite("test_explain_tpch_sf_1_q5", "tpch_sf1") {
    explain {
            sql """
		SELECT
		  n_name,
		  sum(l_extendedprice * (1 - l_discount)) AS revenue
		FROM
		  customer,
		  orders,
		  lineitem,
		  supplier,
		  nation,
		  region
		WHERE
		  c_custkey = o_custkey
		  AND l_orderkey = o_orderkey
		  AND l_suppkey = s_suppkey
		  AND c_nationkey = s_nationkey
		  AND s_nationkey = n_nationkey
		  AND n_regionkey = r_regionkey
		  AND r_name = 'ASIA'
		  AND o_orderdate >= DATE '1994-01-01'
		  AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
		GROUP BY
		n_name
		ORDER BY
		revenue DESC
            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 18> <slot 17> sum(`l_extendedprice` * (1 - `l_discount`)) DESC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 17> sum(`l_extendedprice` * (1 - `l_discount`)))\n" + 
				"  |  group by: <slot 16> `n_name`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(`l_extendedprice` * (1 - `l_discount`))\n" + 
				"  |  group by: `n_name`") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: `n_regionkey` = `r_regionkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `r_regionkey`") && 
		explainStr.contains("output slot ids: 1 2 0 \n" + 
				"  |  hash output slot ids: 1 2 0 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: `s_nationkey` = `n_nationkey`\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `n_nationkey`") && 
		explainStr.contains("output slot ids: 1 2 0 12 \n" + 
				"  |  hash output slot ids: 1 2 0 12 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: `o_custkey` = `c_custkey`\n" + 
				"  |  equal join conjunct: `s_nationkey` = `c_nationkey`\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `c_custkey`, RF003[in_or_bloom] <- `c_nationkey`") && 
		explainStr.contains("output slot ids: 1 2 10 \n" + 
				"  |  hash output slot ids: 1 2 10 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_suppkey` = `s_suppkey`\n" + 
				"  |  runtime filters: RF004[in_or_bloom] <- `s_suppkey`") && 
		explainStr.contains("output slot ids: 1 2 4 10 \n" + 
				"  |  hash output slot ids: 1 2 4 10 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_orderkey` = `o_orderkey`\n" + 
				"  |  runtime filters: RF005[in_or_bloom] <- `o_orderkey`") && 
		explainStr.contains("output slot ids: 1 2 7 4 \n" + 
				"  |  hash output slot ids: 1 2 7 4 ") && 
		explainStr.contains("TABLE: lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF004[in_or_bloom] -> `l_suppkey`, RF005[in_or_bloom] -> `l_orderkey`") && 
		explainStr.contains("TABLE: region(region), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `r_name` = 'ASIA'") && 
		explainStr.contains("TABLE: nation(nation), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `n_regionkey`") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: supplier(supplier), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `s_nationkey`, RF003[in_or_bloom] -> `s_nationkey`") && 
		explainStr.contains("TABLE: orders(orders), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `o_orderdate` >= '1994-01-01 00:00:00', `o_orderdate` < '1995-01-01 00:00:00'\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `o_custkey`") 
            
        }
    }
}