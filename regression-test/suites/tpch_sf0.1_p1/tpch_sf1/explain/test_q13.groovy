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

suite("test_explain_tpch_sf_1_q13") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  c_count,
		  count(*) AS custdist
		FROM (
		       SELECT
		         c_custkey,
		         count(o_orderkey) AS c_count
		       FROM
		         customer
		         LEFT OUTER JOIN orders ON
		                                  c_custkey = o_custkey
		                                  AND o_comment NOT LIKE '%special%requests%'
		       GROUP BY
		         c_custkey
		     ) AS c_orders
		GROUP BY
		  c_count
		ORDER BY
		  custdist DESC,
		  c_count DESC

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 10> <slot 9> count(*) DESC, <slot 11> <slot 8> `c_count` DESC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 9> count(*))\n" + 
				"  |  group by: <slot 8> `c_count`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: <slot 5> count(<slot 15>)") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 5> count(<slot 15>))\n" + 
				"  |  group by: <slot 4> `c_custkey`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: count(<slot 15>)\n" + 
				"  |  group by: <slot 12>") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `c_custkey` = `o_custkey`") && 
		explainStr.contains("vec output tuple id: 6") && 
		explainStr.contains("output slot ids: 12 15 \n" + 
				"  |  hash output slot ids: 0 3 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.orders(orders), PREAGGREGATION: ON\n" + 
				"     PREDICATES: NOT `o_comment` LIKE '%special%requests%'") 
            
        }
    }
}
