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

suite("test_explain_tpch_sf_1_q08", "tpch_sf1") {
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
		explainStr.contains("VAGGREGATE (update finalize)\n" + 
				"  |  output: sum(CASE WHEN <slot 156> = 'BRAZIL' THEN <slot 140> * (1 - <slot 141>) ELSE 0 END), sum(<slot 140> * (1 - <slot 141>))\n" + 
				"  |  group by: year(<slot 152>)") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 137> = `r_regionkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `r_regionkey`") && 
		explainStr.contains("vec output tuple id: 17") && 
		explainStr.contains("output slot ids: 140 141 152 156 \n" + 
				"  |  hash output slot ids: 114 130 115 126 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 111> = `n1`.`n_nationkey`\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `n1`.`n_nationkey`") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 114 115 126 130 137 \n" + 
				"  |  hash output slot ids: 103 91 107 92 14 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 85> = `c_custkey`\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `c_custkey`") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 91 92 103 107 111 \n" + 
				"  |  hash output slot ids: 83 71 87 72 12 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 64> = `n2`.`n_nationkey`\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `n2`.`n_nationkey`") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 71 72 83 85 87 \n" + 
				"  |  hash output slot ids: 66 3 68 54 55 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 45> = `o_orderkey`\n" + 
				"  |  runtime filters: RF004[in_or_bloom] <- `o_orderkey`") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 54 55 64 66 68 \n" + 
				"  |  hash output slot ids: 0 51 41 42 10 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 34> = `s_suppkey`\n" + 
				"  |  runtime filters: RF005[in_or_bloom] <- `s_suppkey`") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 41 42 45 51 \n" + 
				"  |  hash output slot ids: 32 17 35 31 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_partkey` = `p_partkey`\n" + 
				"  |  runtime filters: RF006[in_or_bloom] <- `p_partkey`") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 31 32 34 35 \n" + 
				"  |  hash output slot ids: 1 2 7 8 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.lineitem(lineitem), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts. No AggregateInfo\n" + 
				"     PREDICATES: `default_cluster:regression_test_tpch_sf1_p1.lineitem`.`__DORIS_DELETE_SIGN__` = 0\n" + 
				"     runtime filters: RF004[in_or_bloom] -> <slot 8>, RF005[in_or_bloom] -> <slot 7>, RF006[in_or_bloom] -> `l_partkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.region(region), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `r_name` = 'AMERICA', `default_cluster:regression_test_tpch_sf1_p1.region`.`__DORIS_DELETE_SIGN__` = 0") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.nation(nation), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `n1`.`__DORIS_DELETE_SIGN__` = 0\n" + 
				"     runtime filters: RF000[in_or_bloom] -> <slot 14>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.customer(customer), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `default_cluster:regression_test_tpch_sf1_p1.customer`.`__DORIS_DELETE_SIGN__` = 0\n" + 
				"     runtime filters: RF001[in_or_bloom] -> <slot 12>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.nation(nation), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `n2`.`__DORIS_DELETE_SIGN__` = 0") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.orders(orders), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `o_orderdate` >= '1995-01-01 00:00:00', `o_orderdate` <= '1996-12-31 00:00:00', `default_cluster:regression_test_tpch_sf1_p1.orders`.`__DORIS_DELETE_SIGN__` = 0\n" + 
				"     runtime filters: RF002[in_or_bloom] -> <slot 10>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.supplier(supplier), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `default_cluster:regression_test_tpch_sf1_p1.supplier`.`__DORIS_DELETE_SIGN__` = 0\n" + 
				"     runtime filters: RF003[in_or_bloom] -> <slot 17>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.part(part), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `p_type` = 'ECONOMY ANODIZED STEEL', `default_cluster:regression_test_tpch_sf1_p1.part`.`__DORIS_DELETE_SIGN__` = 0") 
            
        }
    }
}