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

suite("test_explain_tpch_sf_1_q2") {
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
				"  |  equal join conjunct: <slot 78> = <slot 10> min(<slot 109>)\n" + 
				"  |  equal join conjunct: <slot 81> = <slot 9> `ps_partkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- <slot 10> min(<slot 109>), RF001[in_or_bloom] <- <slot 9> `ps_partkey`") && 
		explainStr.contains("vec output tuple id: 19") && 
		explainStr.contains("output slot ids: 121 122 125 126 127 128 129 132 \n" + 
				"  |  hash output slot ids: 81 82 85 86 87 88 89 92 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 77> = `r_regionkey`\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `r_regionkey`") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 78 81 82 85 86 87 88 89 92 \n" + 
				"  |  hash output slot ids: 64 65 68 69 70 71 72 75 61 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 60> = `n_nationkey`\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `n_nationkey`") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 61 64 65 68 69 70 71 72 75 77 \n" + 
				"  |  hash output slot ids: 17 50 51 54 55 56 57 58 29 47 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 42> = `s_suppkey`\n" + 
				"  |  runtime filters: RF004[in_or_bloom] <- `s_suppkey`") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 47 50 51 54 55 56 57 58 60 \n" + 
				"  |  hash output slot ids: 16 19 20 21 40 43 27 44 15 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `ps_partkey` = `p_partkey`\n" + 
				"  |  runtime filters: RF005[in_or_bloom] <- `p_partkey`") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 40 42 43 44 \n" + 
				"  |  hash output slot ids: 18 24 13 14 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.partsupp(partsupp), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> <slot 13>, RF004[in_or_bloom] -> <slot 24>, RF005[in_or_bloom] -> `ps_partkey`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: min(<slot 10> min(<slot 109>))\n" + 
				"  |  group by: <slot 9> `ps_partkey`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: min(<slot 109>)\n" + 
				"  |  group by: <slot 110>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 108> = `r_regionkey`\n" + 
				"  |  runtime filters: RF006[in_or_bloom] <- `r_regionkey`") && 
		explainStr.contains("vec output tuple id: 18") && 
		explainStr.contains("output slot ids: 109 110 \n" + 
				"  |  hash output slot ids: 102 103 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 101> = `n_nationkey`\n" + 
				"  |  runtime filters: RF007[in_or_bloom] <- `n_nationkey`") && 
		explainStr.contains("vec output tuple id: 17") && 
		explainStr.contains("output slot ids: 102 103 108 \n" + 
				"  |  hash output slot ids: 97 98 6 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `ps_suppkey` = `s_suppkey`\n" + 
				"  |  runtime filters: RF008[in_or_bloom] <- `s_suppkey`") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 97 98 101 \n" + 
				"  |  hash output slot ids: 0 1 4 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.partsupp(partsupp), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF008[in_or_bloom] -> `ps_suppkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.region(region), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `r_name` = 'EUROPE'") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.nation(nation), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF006[in_or_bloom] -> <slot 6>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.supplier(supplier), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF007[in_or_bloom] -> <slot 4>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.region(region), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `r_name` = 'EUROPE'") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.nation(nation), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> <slot 29>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.supplier(supplier), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF003[in_or_bloom] -> <slot 27>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.part(part), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `p_size` = 15, `p_type` LIKE '%BRASS'\n" + 
				"     runtime filters: RF001[in_or_bloom] -> <slot 14>")
            
        }
    }
}
