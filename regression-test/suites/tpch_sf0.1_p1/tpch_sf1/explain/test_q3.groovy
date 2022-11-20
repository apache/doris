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

suite("test_explain_tpch_sf_1_q3") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

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
				"  |  order by: <slot 14> <slot 13> sum(<slot 27> * (1 - <slot 28>)) DESC, <slot 15> <slot 11> `o_orderdate` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 13> sum(<slot 27> * (1 - <slot 28>)))\n" + 
				"  |  group by: <slot 10> `l_orderkey`, <slot 11> `o_orderdate`, <slot 12> `o_shippriority`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 27> * (1 - <slot 28>))\n" + 
				"  |  group by: <slot 26>, <slot 30>, <slot 31>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 24> = `c_custkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `c_custkey`") && 
		explainStr.contains("vec output tuple id: 6") && 
		explainStr.contains("output slot ids: 26 27 28 30 31 \n" + 
				"  |  hash output slot ids: 18 19 20 22 23 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_orderkey` = `o_orderkey`\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `o_orderkey`") && 
		explainStr.contains("vec output tuple id: 5") && 
		explainStr.contains("output slot ids: 18 19 20 22 23 24 \n" + 
				"  |  hash output slot ids: 0 1 2 3 4 7 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `l_shipdate` > '1995-03-15 00:00:00'\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `l_orderkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.customer(customer), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `c_mktsegment` = 'BUILDING'") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.orders(orders), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `o_orderdate` < '1995-03-15 00:00:00'\n" + 
				"     runtime filters: RF000[in_or_bloom] -> <slot 7>")
            
        }
    }
}