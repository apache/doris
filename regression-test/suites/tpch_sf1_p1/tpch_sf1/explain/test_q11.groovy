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

suite("test_explain_tpch_sf_1_q11") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  ps_partkey,
		  sum(ps_supplycost * ps_availqty) AS value
		FROM
		  partsupp,
		  supplier,
		  nation
		WHERE
		  ps_suppkey = s_suppkey
		  AND s_nationkey = n_nationkey
		  AND n_name = 'GERMANY'
		GROUP BY
		  ps_partkey
		HAVING
		  sum(ps_supplycost * ps_availqty) > (
		    SELECT sum(ps_supplycost * ps_availqty) * 0.0001
		    FROM
		      partsupp,
		      supplier,
		      nation
		    WHERE
		      ps_suppkey = s_suppkey
		      AND s_nationkey = n_nationkey
		      AND n_name = 'GERMANY'
		  )
		ORDER BY
		  value DESC

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 22> `\$a\$1`.`\$c\$2` DESC") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates: <slot 9> sum(<slot 31> * <slot 32>) > <slot 20> sum(<slot 43> * <slot 44>) * 0.0001") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 9> sum(<slot 31> * <slot 32>))\n" + 
				"  |  group by: <slot 8> `ps_partkey`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 19> sum(<slot 43> * <slot 44>))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: sum(<slot 43> * <slot 44>)\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 42> = `n_nationkey`\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `n_nationkey`") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 43 44 \n" + 
				"  |  hash output slot ids: 38 39 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `ps_suppkey` = `s_suppkey`\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `s_suppkey`") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 38 39 42 \n" + 
				"  |  hash output slot ids: 16 12 13 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.partsupp(partsupp), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF003[in_or_bloom] -> `ps_suppkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.nation(nation), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `n_name` = 'GERMANY'") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.supplier(supplier), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> <slot 16>") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 31> * <slot 32>)\n" + 
				"  |  group by: <slot 30>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 29> = `n_nationkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `n_nationkey`") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 30 31 32 \n" + 
				"  |  hash output slot ids: 24 25 26 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `ps_suppkey` = `s_suppkey`\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `s_suppkey`") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 24 25 26 29 \n" + 
				"  |  hash output slot ids: 0 1 2 5 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.partsupp(partsupp), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `ps_suppkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.nation(nation), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `n_name` = 'GERMANY'") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.supplier(supplier), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> <slot 5>")
            
        }
    }
}
