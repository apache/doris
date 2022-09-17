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

suite("test_explain_tpch_sf_1_q09", "tpch_sf1") {
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
		explainStr.contains("VAGGREGATE (update finalize)\n" + 
				"  |  output: sum(<slot 93> * (1 - <slot 94>) - <slot 103> * <slot 95>)\n" + 
				"  |  group by: <slot 113>, year(<slot 110>)") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 80> = `n_nationkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `n_nationkey`") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 93 94 95 103 110 113 \n" + 
				"  |  hash output slot ids: 0 82 72 73 89 74 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 59> = `o_orderkey`\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `o_orderkey`") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 72 73 74 80 82 89 \n" + 
				"  |  hash output slot ids: 64 1 54 55 56 62 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 43> = `p_partkey`\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `p_partkey`") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 54 55 56 59 62 64 \n" + 
				"  |  hash output slot ids: 49 39 40 41 44 47 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 31> = `ps_suppkey`\n" + 
				"  |  equal join conjunct: <slot 32> = `ps_partkey`\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `ps_suppkey`, RF004[in_or_bloom] <- `ps_partkey`") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 39 40 41 43 44 47 49 \n" + 
				"  |  hash output slot ids: 32 33 36 4 28 29 30 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_suppkey` = `s_suppkey`\n" + 
				"  |  runtime filters: RF005[in_or_bloom] <- `s_suppkey`") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 28 29 30 31 32 33 36 \n" + 
				"  |  hash output slot ids: 2 3 5 7 10 13 14 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.lineitem(lineitem), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts. No AggregateInfo\n" + 
				"     PREDICATES: `default_cluster:regression_test_tpch_sf1_p1.lineitem`.`__DORIS_DELETE_SIGN__` = 0\n" + 
				"     runtime filters: RF001[in_or_bloom] -> <slot 13>, RF002[in_or_bloom] -> <slot 10>, RF003[in_or_bloom] -> <slot 7>, RF004[in_or_bloom] -> <slot 10>, RF005[in_or_bloom] -> `l_suppkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.nation(nation), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `default_cluster:regression_test_tpch_sf1_p1.nation`.`__DORIS_DELETE_SIGN__` = 0") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.orders(orders), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `default_cluster:regression_test_tpch_sf1_p1.orders`.`__DORIS_DELETE_SIGN__` = 0") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.part(part), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `p_name` LIKE '%green%', `default_cluster:regression_test_tpch_sf1_p1.part`.`__DORIS_DELETE_SIGN__` = 0") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.partsupp(partsupp), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `default_cluster:regression_test_tpch_sf1_p1.partsupp`.`__DORIS_DELETE_SIGN__` = 0") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.supplier(supplier), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `default_cluster:regression_test_tpch_sf1_p1.supplier`.`__DORIS_DELETE_SIGN__` = 0\n" + 
				"     runtime filters: RF000[in_or_bloom] -> <slot 14>") 
            
        }
    }
}