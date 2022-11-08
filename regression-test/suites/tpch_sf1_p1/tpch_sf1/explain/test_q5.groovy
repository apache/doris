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

suite("test_explain_tpch_sf_1_q5") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

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
				"  |  order by: <slot 18> <slot 17> sum(<slot 61> * (1 - <slot 62>)) DESC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 17> sum(<slot 61> * (1 - <slot 62>)))\n" + 
				"  |  group by: <slot 16> `n_name`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 61> * (1 - <slot 62>))\n" + 
				"  |  group by: <slot 72>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 60> = `r_regionkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `r_regionkey`") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 61 62 72 \n" + 
				"  |  hash output slot ids: 48 58 47 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 44> = `n_nationkey`\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `n_nationkey`") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 47 48 58 60 \n" + 
				"  |  hash output slot ids: 0 36 37 12 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 31> = `c_custkey`\n" + 
				"  |  equal join conjunct: <slot 35> = `c_nationkey`\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `c_custkey`, RF003[in_or_bloom] <- `c_nationkey`") &&  
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 36 37 44 \n" + 
				"  |  hash output slot ids: 35 27 28 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 23> = `s_suppkey`\n" + 
				"  |  runtime filters: RF004[in_or_bloom] <- `s_suppkey`") &&  
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 27 28 31 35 \n" + 
				"  |  hash output slot ids: 20 21 24 10 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_orderkey` = `o_orderkey`\n" + 
				"  |  runtime filters: RF005[in_or_bloom] <- `o_orderkey`") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 20 21 23 24 \n" + 
				"  |  hash output slot ids: 1 2 4 7 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF004[in_or_bloom] -> <slot 7>, RF005[in_or_bloom] -> `l_orderkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.region(region), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `r_name` = 'ASIA'") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.nation(nation), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> <slot 12>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.supplier(supplier), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> <slot 10>, RF003[in_or_bloom] -> <slot 10>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.orders(orders), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `o_orderdate` >= '1994-01-01 00:00:00', `o_orderdate` < '1995-01-01 00:00:00'\n" + 
				"     runtime filters: RF002[in_or_bloom] -> <slot 4>")
            
        }
    }
}