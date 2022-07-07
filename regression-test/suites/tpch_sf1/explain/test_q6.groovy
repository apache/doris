suite("test_explain_tpch_sf_1_q6", "tpch_sf1") {
    explain {
            sql """
		SELECT sum(l_extendedprice * l_discount) AS revenue
		FROM
		  lineitem
		WHERE
		  l_shipdate >= DATE '1994-01-01'
		  AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
		AND l_discount BETWEEN 0.06 - 0.01 AND .06 + 0.01
		AND l_quantity < 24

            """
        check {
            explainStr ->
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 4> sum(`l_extendedprice` * `l_discount`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: sum(`l_extendedprice` * `l_discount`)\n" + 
				"  |  group by: ") && 
		explainStr.contains("TABLE: lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `l_shipdate` >= '1994-01-01 00:00:00', `l_shipdate` < '1995-01-01 00:00:00', `l_discount` >= 0.05, `l_discount` <= 0.07, `l_quantity` < 24") 
            
        }
    }
}