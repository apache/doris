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

suite("test_explain_tpch_sf_1_q17") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
		FROM
		  lineitem,
		  part
		WHERE
		  p_partkey = l_partkey
		  AND p_brand = 'Brand#23'
		  AND p_container = 'MED BOX'
		  AND l_quantity < (
		    SELECT 0.2 * avg(l_quantity)
		    FROM
		      lineitem
		    WHERE
		      l_partkey = p_partkey
		  )

            """
        check {
            explainStr ->
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 12> sum(<slot 21>))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: sum(<slot 21>)\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 17> = <slot 2> `l_partkey`\n" + 
				"  |  other join predicates: <slot 32> < 0.2 * <slot 36>\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- <slot 2> `l_partkey`") && 
		explainStr.contains("other join predicates: <slot 32> < 0.2 * <slot 36>") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 21 \n" + 
				"  |  hash output slot ids: 3 14 15 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_partkey` = `p_partkey`\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `p_partkey`") && 
		explainStr.contains("vec output tuple id: 7") && 
		explainStr.contains("output slot ids: 14 15 17 \n" + 
				"  |  hash output slot ids: 6 7 8 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `l_partkey`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: avg(<slot 3> avg(`l_quantity`))\n" + 
				"  |  group by: <slot 2> `l_partkey`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(`l_quantity`)\n" + 
				"  |  group by: `l_partkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.lineitem(lineitem), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.part(part), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `p_brand` = 'Brand#23', `p_container` = 'MED BOX'\n" + 
				"     runtime filters: RF000[in_or_bloom] -> <slot 7>")
            
        }
    }
}
