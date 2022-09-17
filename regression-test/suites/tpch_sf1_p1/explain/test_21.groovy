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

suite("test_explain_tpch_sf_1_q21", "tpch_sf1") {
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
		explainStr.contains("VAGGREGATE (update finalize)\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: <slot 145>") && 
		explainStr.contains("join op: LEFT ANTI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 125> = `l3`.`l_orderkey`\n" + 
				"  |  other join predicates: <slot 188> != <slot 184>") && 
		explainStr.contains("other join predicates: <slot 188> != <slot 184>") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 145 \n" + 
				"  |  hash output slot ids: 129 37 124 ") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 109> = `l2`.`l_orderkey`\n" + 
				"  |  other join predicates: <slot 182> != <slot 178>") && 
		explainStr.contains("other join predicates: <slot 182> != <slot 178>\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `l2`.`l_orderkey`") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 124 125 129 \n" + 
				"  |  hash output slot ids: 113 1 108 109 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 102> = `n_nationkey`\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `n_nationkey`") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 108 109 113 \n" + 
				"  |  hash output slot ids: 96 100 95 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 86> = `o_orderkey`\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `o_orderkey`") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 95 96 100 102 \n" + 
				"  |  hash output slot ids: 85 86 90 92 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l1`.`l_suppkey` = `s_suppkey`\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `s_suppkey`") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 85 86 90 92 \n" + 
				"  |  hash output slot ids: 34 35 70 76 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.lineitem(lineitem), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts. conjunct on `L_RECEIPTDATE` which is StorageEngine value column\n" + 
				"     PREDICATES: `l1`.`l_receiptdate` > `l1`.`l_commitdate`, `l1`.`__DORIS_DELETE_SIGN__` = 0\n" + 
				"     runtime filters: RF000[in_or_bloom] -> <slot 35>, RF002[in_or_bloom] -> <slot 35>, RF003[in_or_bloom] -> `l1`.`l_suppkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.lineitem(lineitem), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts. No AggregateInfo\n" + 
				"     PREDICATES: `l3`.`l_receiptdate` > `l3`.`l_commitdate`, `l3`.`__DORIS_DELETE_SIGN__` = 0") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.lineitem(lineitem), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts. No AggregateInfo\n" + 
				"     PREDICATES: `l2`.`__DORIS_DELETE_SIGN__` = 0") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.nation(nation), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `n_name` = 'SAUDI ARABIA', `default_cluster:regression_test_tpch_sf1_p1.nation`.`__DORIS_DELETE_SIGN__` = 0") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.orders(orders), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `o_orderstatus` = 'F', `default_cluster:regression_test_tpch_sf1_p1.orders`.`__DORIS_DELETE_SIGN__` = 0") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.supplier(supplier), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `default_cluster:regression_test_tpch_sf1_p1.supplier`.`__DORIS_DELETE_SIGN__` = 0\n" + 
				"     runtime filters: RF001[in_or_bloom] -> <slot 76>") 
            
        }
    }
}