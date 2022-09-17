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

suite("test_explain_tpch_sf_1_q02", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  s_acctbal,
		  s_name,
		  n_name,
		  p_partkey,
		  p_mfgr,
		  s_address,
		  s_phone,
		  s_comment
		FROM
		  part,
		  supplier,
		  partsupp,
		  nation,
		  region
		WHERE
		  p_partkey = ps_partkey
		  AND s_suppkey = ps_suppkey
		  AND p_size = 15
		  AND p_type LIKE '%BRASS'
		  AND s_nationkey = n_nationkey
		  AND n_regionkey = r_regionkey
		  AND r_name = 'EUROPE'
		  AND ps_supplycost = (
		    SELECT min(ps_supplycost)
		    FROM
		      partsupp, supplier,
		      nation, region
		    WHERE
		      p_partkey = ps_partkey
		      AND s_suppkey = ps_suppkey
		      AND s_nationkey = n_nationkey
		      AND n_regionkey = r_regionkey
		      AND r_name = 'EUROPE'
		  )
		ORDER BY
		  s_acctbal DESC,
		  n_name,
		  s_name,
		  p_partkey
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 32> `s_acctbal` DESC, <slot 33> `n_name` ASC, <slot 34> `s_name` ASC, <slot 35> `p_partkey` ASC") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 92> = <slot 10> min(<slot 137>)\n" + 
				"  |  equal join conjunct: <slot 96> = <slot 9> `ps_partkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- <slot 10> min(<slot 137>), RF001[in_or_bloom] <- <slot 9> `ps_partkey`") && 
		explainStr.contains("vec output tuple id: 19") && 
		explainStr.contains("output slot ids: 154 155 159 160 161 162 163 167 \n" + 
				"  |  hash output slot ids: 96 97 101 102 103 104 105 109 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 89> = `r_regionkey`\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `r_regionkey`") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 92 96 97 101 102 103 104 105 109 \n" + 
				"  |  hash output slot ids: 80 81 82 83 70 87 74 75 79 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 67> = `n_nationkey`\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `n_nationkey`") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 70 74 75 79 80 81 82 83 87 89 \n" + 
				"  |  hash output slot ids: 64 65 17 52 56 57 61 29 62 63 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 44> = `s_suppkey`\n" + 
				"  |  runtime filters: RF004[in_or_bloom] <- `s_suppkey`") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 52 56 57 61 62 63 64 65 67 \n" + 
				"  |  hash output slot ids: 16 19 20 21 42 27 46 47 15 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `ps_partkey` = `p_partkey`\n" + 
				"  |  runtime filters: RF005[in_or_bloom] <- `p_partkey`") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 42 44 46 47 \n" + 
				"  |  hash output slot ids: 18 24 13 14 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.partsupp(partsupp), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts. No AggregateInfo\n" + 
				"     PREDICATES: `default_cluster:regression_test_tpch_sf1_p1.partsupp`.`__DORIS_DELETE_SIGN__` = 0\n" + 
				"     runtime filters: RF000[in_or_bloom] -> <slot 13>, RF004[in_or_bloom] -> <slot 24>, RF005[in_or_bloom] -> `ps_partkey`") && 
		explainStr.contains("VAGGREGATE (update finalize)\n" + 
				"  |  output: min(<slot 137>)\n" + 
				"  |  group by: <slot 138>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 134> = `r_regionkey`\n" + 
				"  |  runtime filters: RF006[in_or_bloom] <- `r_regionkey`") && 
		explainStr.contains("vec output tuple id: 18") && 
		explainStr.contains("output slot ids: 137 138 \n" + 
				"  |  hash output slot ids: 126 127 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 123> = `n_nationkey`\n" + 
				"  |  runtime filters: RF007[in_or_bloom] <- `n_nationkey`") && 
		explainStr.contains("vec output tuple id: 17") && 
		explainStr.contains("output slot ids: 126 127 134 \n" + 
				"  |  hash output slot ids: 118 6 119 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `ps_suppkey` = `s_suppkey`\n" + 
				"  |  runtime filters: RF008[in_or_bloom] <- `s_suppkey`") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 118 119 123 \n" + 
				"  |  hash output slot ids: 0 1 4 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.partsupp(partsupp), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts. Aggregate Operator not match: MIN <--> REPLACE\n" + 
				"     PREDICATES: `default_cluster:regression_test_tpch_sf1_p1.partsupp`.`__DORIS_DELETE_SIGN__` = 0\n" + 
				"     runtime filters: RF008[in_or_bloom] -> `ps_suppkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.region(region), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `r_name` = 'EUROPE', `default_cluster:regression_test_tpch_sf1_p1.region`.`__DORIS_DELETE_SIGN__` = 0") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.nation(nation), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `default_cluster:regression_test_tpch_sf1_p1.nation`.`__DORIS_DELETE_SIGN__` = 0\n" + 
				"     runtime filters: RF006[in_or_bloom] -> <slot 6>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.supplier(supplier), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `default_cluster:regression_test_tpch_sf1_p1.supplier`.`__DORIS_DELETE_SIGN__` = 0\n" + 
				"     runtime filters: RF007[in_or_bloom] -> <slot 4>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.region(region), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `r_name` = 'EUROPE', `default_cluster:regression_test_tpch_sf1_p1.region`.`__DORIS_DELETE_SIGN__` = 0") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.nation(nation), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `default_cluster:regression_test_tpch_sf1_p1.nation`.`__DORIS_DELETE_SIGN__` = 0\n" + 
				"     runtime filters: RF002[in_or_bloom] -> <slot 29>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.supplier(supplier), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `default_cluster:regression_test_tpch_sf1_p1.supplier`.`__DORIS_DELETE_SIGN__` = 0\n" + 
				"     runtime filters: RF003[in_or_bloom] -> <slot 27>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.part(part), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `p_size` = 15, `p_type` LIKE '%BRASS', `default_cluster:regression_test_tpch_sf1_p1.part`.`__DORIS_DELETE_SIGN__` = 0\n" + 
				"     runtime filters: RF001[in_or_bloom] -> <slot 14>") 
            
        }
    }
}