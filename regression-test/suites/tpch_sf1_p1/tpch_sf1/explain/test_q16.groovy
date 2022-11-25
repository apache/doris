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

suite("test_explain_tpch_sf_1_q16") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  p_brand,
		  p_type,
		  p_size,
		  count(DISTINCT ps_suppkey) AS supplier_cnt
		FROM
		  partsupp,
		  part
		WHERE
		  p_partkey = ps_partkey
		  AND p_brand <> 'Brand#45'
		  AND p_type NOT LIKE 'MEDIUM POLISHED%'
		  AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
		  AND ps_suppkey NOT IN (
		    SELECT s_suppkey
		    FROM
		      supplier
		    WHERE
		      s_comment LIKE '%Customer%Complaints%'
		  )
		GROUP BY
		  p_brand,
		  p_type,
		  p_size
		ORDER BY
		  supplier_cnt DESC,
		  p_brand,
		  p_type,
		  p_size

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 17> <slot 16> count(<slot 12> `ps_suppkey`) DESC, <slot 18> <slot 13> <slot 9> `p_brand` ASC, <slot 19> <slot 14> <slot 10> `p_type` ASC, <slot 20> <slot 15> <slot 11> `p_size` ASC") && 
		explainStr.contains("VAGGREGATE (update finalize)\n" + 
				"  |  output: count(<slot 12> `ps_suppkey`)\n" + 
				"  |  group by: <slot 9> `p_brand`, <slot 10> `p_type`, <slot 11> `p_size`") && 
		explainStr.contains("VAGGREGATE (merge serialize)\n" + 
				"  |  group by: <slot 9> `p_brand`, <slot 10> `p_type`, <slot 11> `p_size`, <slot 12> `ps_suppkey`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  group by: <slot 29>, <slot 30>, <slot 31>, <slot 27>") && 
		explainStr.contains("join op: NULL AWARE LEFT ANTI JOIN(BROADCAST)[Build side of null aware left anti join must be broadcast]\n" +
				"  |  equal join conjunct: <slot 21> = `s_suppkey`") && 
		explainStr.contains("vec output tuple id: 8") &&
		explainStr.contains("output slot ids: 27 29 30 31 \n" + 
				"  |  hash output slot ids: 21 23 24 25 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `ps_partkey` = `p_partkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `p_partkey`") && 
		explainStr.contains("vec output tuple id: 7") && 
		explainStr.contains("output slot ids: 21 23 24 25 \n" + 
				"  |  hash output slot ids: 3 4 5 6 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.partsupp(partsupp), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ps_partkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.supplier(supplier), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `s_comment` LIKE '%Customer%Complaints%'") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.part(part), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `p_brand` != 'Brand#45', NOT `p_type` LIKE 'MEDIUM POLISHED%', `p_size` IN (49, 14, 23, 45, 19, 3, 36, 9)") 
            
        }
    }
}
