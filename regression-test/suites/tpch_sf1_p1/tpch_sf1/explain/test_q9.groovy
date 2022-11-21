// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_explain_tpch_sf_1_q9") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  nation,
		  o_year,
		  sum(amount) AS sum_profit
		FROM (
		       SELECT
		         n_name                                                          AS nation,
		         extract(YEAR FROM o_orderdate)                                  AS o_year,
		         l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
		       FROM
		         part,
		         supplier,
		         lineitem,
		         partsupp,
		         orders,
		         nation
		       WHERE
		         s_suppkey = l_suppkey
		         AND ps_suppkey = l_suppkey
		         AND ps_partkey = l_partkey
		         AND p_partkey = l_partkey
		         AND o_orderkey = l_orderkey
		         AND s_nationkey = n_nationkey
		         AND p_name LIKE '%green%'
		     ) AS profit
		GROUP BY
		  nation,
		  o_year
		ORDER BY
		  nation,
		  o_year DESC

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 23> <slot 20> `nation` ASC, <slot 24> <slot 21> `o_year` DESC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 22> sum(<slot 73> * (1 - <slot 74>) - <slot 81> * <slot 75>))\n" + 
				"  |  group by: <slot 20> `nation`, <slot 21> `o_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 73> * (1 - <slot 74>) - <slot 81> * <slot 75>)\n" + 
				"  |  group by: <slot 88>, year(<slot 86>)") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 65> = `n_nationkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `n_nationkey`") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 73 74 75 81 86 88 \n" + 
				"  |  hash output slot ids: 0 66 71 58 59 60 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 50> = `o_orderkey`\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `o_orderkey`") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 58 59 60 65 66 71 \n" + 
				"  |  hash output slot ids: 1 52 53 45 46 47 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 38> = `p_partkey`\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `p_partkey`") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 45 46 47 50 52 53 \n" + 
				"  |  hash output slot ids: 34 35 36 39 41 42 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 29> = `ps_suppkey`\n" + 
				"  |  equal join conjunct: <slot 30> = `ps_partkey`\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `ps_suppkey`, RF004[in_or_bloom] <- `ps_partkey`") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 34 35 36 38 39 41 42 \n" + 
				"  |  hash output slot ids: 33 4 26 27 28 30 31 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_suppkey` = `s_suppkey`\n" + 
				"  |  runtime filters: RF005[in_or_bloom] <- `s_suppkey`") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 26 27 28 29 30 31 33 \n" + 
				"  |  hash output slot ids: 2 3 5 7 10 13 14 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> <slot 13>, RF002[in_or_bloom] -> <slot 10>, RF003[in_or_bloom] -> <slot 7>, RF004[in_or_bloom] -> <slot 10>, RF005[in_or_bloom] -> `l_suppkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.nation(nation), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.orders(orders), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.part(part), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `p_name` LIKE '%green%'") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.partsupp(partsupp), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.supplier(supplier), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> <slot 14>")
            
        }
    }
}