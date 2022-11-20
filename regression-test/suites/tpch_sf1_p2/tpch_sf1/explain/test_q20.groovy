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

suite("test_explain_tpch_sf_1_q20") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  s_name,
		  s_address
		FROM
		  supplier, nation
		WHERE
		  s_suppkey IN (
		    SELECT ps_suppkey
		    FROM
		      partsupp
		    WHERE
		      ps_partkey IN (
		        SELECT p_partkey
		        FROM
		          part
		        WHERE
		          p_name LIKE 'forest%'
		      )
		      AND ps_availqty > (
		        SELECT 0.5 * sum(l_quantity)
		        FROM
		          lineitem
		        WHERE
		          l_partkey = ps_partkey
		          AND l_suppkey = ps_suppkey
		          AND l_shipdate >= date('1994-01-01')
		          AND l_shipdate < date('1994-01-01') + interval '1' YEAR
		)
		)
		AND s_nationkey = n_nationkey
		AND n_name = 'CANADA'
		ORDER BY s_name

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 23> `s_name` ASC") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 25> = <slot 36>\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- <slot 36>") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 38 39 \n" + 
				"  |  hash output slot ids: 26 27 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `s_nationkey` = `n_nationkey`\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `n_nationkey`") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 25 26 27 \n" + 
				"  |  hash output slot ids: 17 18 19 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.supplier(supplier), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> <slot 17>, RF001[in_or_bloom] -> `s_nationkey`") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 33> = <slot 9> `l_suppkey`\n" + 
				"  |  equal join conjunct: <slot 31> = <slot 8> `l_partkey`\n" + 
				"  |  other join predicates: <slot 53> > 0.5 * <slot 57>\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- <slot 9> `l_suppkey`, RF003[in_or_bloom] <- <slot 8> `l_partkey`") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 36 \n" + 
				"  |  hash output slot ids: 32 33 10 ") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `ps_partkey` = `p_partkey`\n" + 
				"  |  runtime filters: RF004[in_or_bloom] <- `p_partkey`") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 31 32 33 \n" + 
				"  |  hash output slot ids: 3 14 15 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.partsupp(partsupp), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> <slot 15>, RF003[in_or_bloom] -> <slot 3>, RF004[in_or_bloom] -> `ps_partkey`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 10> sum(`l_quantity`))\n" + 
				"  |  group by: <slot 8> `l_partkey`, <slot 9> `l_suppkey`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(`l_quantity`)\n" + 
				"  |  group by: `l_partkey`, `l_suppkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `l_shipdate` >= date('1994-01-01 00:00:00'), `l_shipdate` < date('1994-01-01 00:00:00') + INTERVAL 1 YEAR") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.part(part), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `p_name` LIKE 'forest%'") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.nation(nation), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `n_name` = 'CANADA'") 
            
        }
    }
}