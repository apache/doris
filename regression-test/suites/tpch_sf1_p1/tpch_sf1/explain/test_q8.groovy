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

suite("test_explain_tpch_sf_1_q8") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  o_year,
		  sum(CASE
		      WHEN nation = 'BRAZIL'
		        THEN volume
		      ELSE 0
		      END) / sum(volume) AS mkt_share
		FROM (
		       SELECT
		         extract(YEAR FROM o_orderdate)     AS o_year,
		         l_extendedprice * (1 - l_discount) AS volume,
		         n2.n_name                          AS nation
		       FROM
		         part,
		         supplier,
		         lineitem,
		         orders,
		         customer,
		         nation n1,
		         nation n2,
		         region
		       WHERE
		         p_partkey = l_partkey
		         AND s_suppkey = l_suppkey
		         AND l_orderkey = o_orderkey
		         AND o_custkey = c_custkey
		         AND c_nationkey = n1.n_nationkey
		         AND n1.n_regionkey = r_regionkey
		         AND r_name = 'AMERICA'
		         AND s_nationkey = n2.n_nationkey
		         AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
		         AND p_type = 'ECONOMY ANODIZED STEEL'
		     ) AS all_nations
		GROUP BY
		  o_year
		ORDER BY
		  o_year

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 26> <slot 23> `o_year` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 24> sum(CASE WHEN <slot 117> = 'BRAZIL' THEN <slot 105> * (1 - <slot 106>) ELSE 0 END)), sum(<slot 25> sum(<slot 105> * (1 - <slot 106>)))\n" + 
				"  |  group by: <slot 23> `o_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(CASE WHEN <slot 117> = 'BRAZIL' THEN <slot 105> * (1 - <slot 106>) ELSE 0 END), sum(<slot 105> * (1 - <slot 106>))\n" + 
				"  |  group by: year(<slot 114>)") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 104> = `r_regionkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `r_regionkey`") && 
		explainStr.contains("vec output tuple id: 17") && 
		explainStr.contains("output slot ids: 105 106 114 117 \n" + 
				"  |  hash output slot ids: 96 99 87 88 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 86> = `n1`.`n_nationkey`\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `n1`.`n_nationkey`") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 87 88 96 99 104 \n" + 
				"  |  hash output slot ids: 80 83 71 72 14 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 68> = `c_custkey`\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `c_custkey`") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 71 72 80 83 86 \n" + 
				"  |  hash output slot ids: 66 69 57 58 12 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 53> = `n2`.`n_nationkey`\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `n2`.`n_nationkey`") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 57 58 66 68 69 \n" + 
				"  |  hash output slot ids: 3 54 56 45 46 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 40> = `o_orderkey`\n" + 
				"  |  runtime filters: RF004[in_or_bloom] <- `o_orderkey`") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 45 46 53 54 56 \n" + 
				"  |  hash output slot ids: 0 36 37 10 44 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 32> = `s_suppkey`\n" + 
				"  |  runtime filters: RF005[in_or_bloom] <- `s_suppkey`") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 36 37 40 44 \n" + 
				"  |  hash output slot ids: 33 17 29 30 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_partkey` = `p_partkey`\n" + 
				"  |  runtime filters: RF006[in_or_bloom] <- `p_partkey`") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 29 30 32 33 \n" + 
				"  |  hash output slot ids: 1 2 7 8 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF004[in_or_bloom] -> <slot 8>, RF005[in_or_bloom] -> <slot 7>, RF006[in_or_bloom] -> `l_partkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.region(region), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `r_name` = 'AMERICA'") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.nation(nation), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> <slot 14>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.customer(customer), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> <slot 12>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.nation(nation), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.orders(orders), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `o_orderdate` >= '1995-01-01 00:00:00', `o_orderdate` <= '1996-12-31 00:00:00'\n" + 
				"     runtime filters: RF002[in_or_bloom] -> <slot 10>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.supplier(supplier), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF003[in_or_bloom] -> <slot 17>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.part(part), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `p_type` = 'ECONOMY ANODIZED STEEL'") 
            
        }
    }
}