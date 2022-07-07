suite("test_explain_tpch_sf_1_q10", "tpch_sf1") {
    explain {
            sql """
		SELECT
		  c_custkey,
		  c_name,
		  sum(l_extendedprice * (1 - l_discount)) AS revenue,
		  c_acctbal,
		  n_name,
		  c_address,
		  c_phone,
		  c_comment
		FROM
		  customer,
		  orders,
		  lineitem,
		  nation
		WHERE
		  c_custkey = o_custkey
		  AND l_orderkey = o_orderkey
		  AND o_orderdate >= DATE '1993-10-01'
		  AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH
		  AND l_returnflag = 'R'
		  AND c_nationkey = n_nationkey
		GROUP BY
		  c_custkey,
		  c_name,
		  c_acctbal,
		  c_phone,
		  n_name,
		  c_address,
		  c_comment
		ORDER BY
		  revenue DESC
		LIMIT 20
            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 24> <slot 23> sum(`l_extendedprice` * (1 - `l_discount`)) DESC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 23> sum(`l_extendedprice` * (1 - `l_discount`)))\n" + 
				"  |  group by: <slot 16> `c_custkey`, <slot 17> `c_name`, <slot 18> `c_acctbal`, <slot 19> `c_phone`, <slot 20> `n_name`, <slot 21> `c_address`, <slot 22> `c_comment`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(`l_extendedprice` * (1 - `l_discount`))\n" + 
				"  |  group by: `c_custkey`, `c_name`, `c_acctbal`, `c_phone`, `n_name`, `c_address`, `c_comment`") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: `c_nationkey` = `n_nationkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `n_nationkey`") && 
		explainStr.contains("output slot ids: 2 3 0 1 4 6 7 8 5 \n" + 
				"  |  hash output slot ids: 2 3 0 1 4 6 7 8 5 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: `o_custkey` = `c_custkey`\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `c_custkey`") && 
		explainStr.contains("output slot ids: 2 3 0 1 4 6 7 8 14 \n" + 
				"  |  hash output slot ids: 2 3 0 1 4 6 7 8 14 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_orderkey` = `o_orderkey`\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `o_orderkey`") && 
		explainStr.contains("output slot ids: 2 3 9 \n" + 
				"  |  hash output slot ids: 2 3 9 ") && 
		explainStr.contains("TABLE: lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `l_returnflag` = 'R'\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `l_orderkey`") && 
		explainStr.contains("TABLE: nation(nation), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `c_nationkey`") && 
		explainStr.contains("TABLE: orders(orders), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `o_orderdate` >= '1993-10-01 00:00:00', `o_orderdate` < '1994-01-01 00:00:00'\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `o_custkey`") 
            
        }
    }
}