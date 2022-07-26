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

suite("test_explain_tpch_sf_1_q2", "tpch_sf1") {
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
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[Src column hash been transformed by expr]\n" + 
				"  |  equal join conjunct: `ps_supplycost` = <slot 10> min(`ps_supplycost`)\n" + 
				"  |  equal join conjunct: `p_partkey` = <slot 9> `ps_partkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- <slot 10> min(`ps_supplycost`), RF001[in_or_bloom] <- <slot 9> `ps_partkey`") && 
		explainStr.contains("output slot ids: 14 18 15 16 19 20 21 17 \n" + 
				"  |  hash output slot ids: 14 18 15 16 19 20 21 17 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: `n_regionkey` = `r_regionkey`\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `r_regionkey`") && 
		explainStr.contains("output slot ids: 13 14 18 15 16 19 20 21 17 \n" + 
				"  |  hash output slot ids: 13 14 18 15 16 19 20 21 17 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: `s_nationkey` = `n_nationkey`\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `n_nationkey`") && 
		explainStr.contains("output slot ids: 13 14 18 15 16 19 20 21 17 29 \n" + 
				"  |  hash output slot ids: 13 14 18 15 16 19 20 21 17 29 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `ps_suppkey` = `s_suppkey`\n" + 
				"  |  runtime filters: RF004[in_or_bloom] <- `s_suppkey`") && 
		explainStr.contains("output slot ids: 13 14 18 15 16 19 20 21 27 \n" + 
				"  |  hash output slot ids: 13 14 18 15 16 19 20 21 27 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `ps_partkey` = `p_partkey`\n" + 
				"  |  runtime filters: RF005[in_or_bloom] <- `p_partkey`") && 
		explainStr.contains("output slot ids: 13 24 14 18 \n" + 
				"  |  hash output slot ids: 13 24 14 18 ") && 
		explainStr.contains("TABLE: partsupp(partsupp), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ps_supplycost`, RF004[in_or_bloom] -> `ps_suppkey`, RF005[in_or_bloom] -> `ps_partkey`") && 
		explainStr.contains("VAGGREGATE (update finalize)\n" + 
				"  |  output: min(`ps_supplycost`)\n" + 
				"  |  group by: `ps_partkey`") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: `n_regionkey` = `r_regionkey`\n" + 
				"  |  runtime filters: RF006[in_or_bloom] <- `r_regionkey`") && 
		explainStr.contains("output slot ids: 0 1 \n" + 
				"  |  hash output slot ids: 0 1 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: `s_nationkey` = `n_nationkey`\n" + 
				"  |  runtime filters: RF007[in_or_bloom] <- `n_nationkey`") && 
		explainStr.contains("output slot ids: 0 1 6 \n" + 
				"  |  hash output slot ids: 0 1 6 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `ps_suppkey` = `s_suppkey`\n" + 
				"  |  runtime filters: RF008[in_or_bloom] <- `s_suppkey`") && 
		explainStr.contains("output slot ids: 0 1 4 \n" + 
				"  |  hash output slot ids: 0 1 4 ") && 
		explainStr.contains("TABLE: partsupp(partsupp), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF008[in_or_bloom] -> `ps_suppkey`") && 
		explainStr.contains("TABLE: region(region), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `r_name` = 'EUROPE'") && 
		explainStr.contains("TABLE: nation(nation), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF006[in_or_bloom] -> `n_regionkey`") && 
		explainStr.contains("TABLE: supplier(supplier), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF007[in_or_bloom] -> `s_nationkey`") && 
		explainStr.contains("TABLE: region(region), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `r_name` = 'EUROPE'") && 
		explainStr.contains("TABLE: nation(nation), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `n_regionkey`") && 
		explainStr.contains("TABLE: supplier(supplier), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF003[in_or_bloom] -> `s_nationkey`") && 
		explainStr.contains("TABLE: part(part), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `p_size` = 15, `p_type` LIKE '%BRASS'\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `p_partkey`") 
            
        }
    }
}