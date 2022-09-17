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

suite("test_explain_tpch_sf_1_q04", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  o_orderpriority,
		  count(*) AS order_count
		FROM orders
		WHERE
		  o_orderdate >= DATE '1993-07-01'
		  AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
		AND EXISTS (
		SELECT *
		FROM lineitem
		WHERE
		l_orderkey = o_orderkey
		AND l_commitdate < l_receiptdate
		)
		GROUP BY
		o_orderpriority
		ORDER BY
		o_orderpriority

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 38> <slot 36> `o_orderpriority` ASC") && 
		explainStr.contains("VAGGREGATE (update finalize)\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: <slot 43>") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `o_orderkey` = `l_orderkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `l_orderkey`") && 
		explainStr.contains("vec output tuple id: 5") && 
		explainStr.contains("output slot ids: 43 \n" + 
				"  |  hash output slot ids: 34 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.orders(orders), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts. conjunct on `O_ORDERPRIORITY` which is StorageEngine value column\n" + 
				"     PREDICATES: `o_orderdate` >= '1993-07-01 00:00:00', `o_orderdate` < '1993-10-01 00:00:00', `default_cluster:regression_test_tpch_sf1_p1.orders`.`__DORIS_DELETE_SIGN__` = 0\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `o_orderkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.lineitem(lineitem), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts. No AggregateInfo\n" + 
				"     PREDICATES: `l_commitdate` < `l_receiptdate`, `default_cluster:regression_test_tpch_sf1_p1.lineitem`.`__DORIS_DELETE_SIGN__` = 0") 
            
        }
    }
}