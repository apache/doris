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

suite("test_explain_tpch_sf_1_q21") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  s_name,
		  count(*) AS numwait
		FROM
		  supplier,
		  lineitem l1,
		  orders,
		  nation
		WHERE
		  s_suppkey = l1.l_suppkey
		  AND o_orderkey = l1.l_orderkey
		  AND o_orderstatus = 'F'
		  AND l1.l_receiptdate > l1.l_commitdate
		  AND exists(
		    SELECT *
		    FROM
		      lineitem l2
		    WHERE
		      l2.l_orderkey = l1.l_orderkey
		      AND l2.l_suppkey <> l1.l_suppkey
		  )
		  AND NOT exists(
		    SELECT *
		    FROM
		      lineitem l3
		    WHERE
		      l3.l_orderkey = l1.l_orderkey
		      AND l3.l_suppkey <> l1.l_suppkey
		      AND l3.l_receiptdate > l3.l_commitdate
		  )
		  AND s_nationkey = n_nationkey
		  AND n_name = 'SAUDI ARABIA'
		GROUP BY
		  s_name
		ORDER BY
		  numwait DESC,
		  s_name
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 81> <slot 80> count(*) DESC, <slot 82> <slot 79> `s_name` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 80> count(*))\n" + 
				"  |  group by: <slot 79> `s_name`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: <slot 125>") && 
		explainStr.contains("join op: LEFT ANTI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 111> = `l3`.`l_orderkey`\n" + 
				"  |  other join predicates: <slot 160> != <slot 156>") && 
		explainStr.contains("other join predicates: <slot 160> != <slot 156>") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 125 \n" + 
				"  |  hash output slot ids: 114 37 110 ") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 100> = `l2`.`l_orderkey`\n" + 
				"  |  other join predicates: <slot 155> != <slot 151>\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `l2`.`l_orderkey`") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 110 111 114 \n" + 
				"  |  hash output slot ids: 1 99 100 103 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 96> = `n_nationkey`\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `n_nationkey`") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 99 100 103 \n" + 
				"  |  hash output slot ids: 90 91 94 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 84> = `o_orderkey`\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `o_orderkey`") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 90 91 94 96 \n" + 
				"  |  hash output slot ids: 83 84 87 89 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l1`.`l_suppkey` = `s_suppkey`\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `s_suppkey`") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 83 84 87 89 \n" + 
				"  |  hash output slot ids: 34 35 70 76 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `l1`.`l_receiptdate` > `l1`.`l_commitdate`\n" + 
				"     runtime filters: RF000[in_or_bloom] -> <slot 35>, RF002[in_or_bloom] -> <slot 35>, RF003[in_or_bloom] -> `l1`.`l_suppkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `l3`.`l_receiptdate` > `l3`.`l_commitdate`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.lineitem(lineitem), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.nation(nation), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `n_name` = 'SAUDI ARABIA'") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.orders(orders), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `o_orderstatus` = 'F'") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.supplier(supplier), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> <slot 76>")
            
        }
    }
}