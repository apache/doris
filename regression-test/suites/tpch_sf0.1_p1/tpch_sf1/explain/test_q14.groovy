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

suite("test_explain_tpch_sf_1_q14") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT 100.00 * sum(CASE
		                    WHEN p_type LIKE 'PROMO%'
		                      THEN l_extendedprice * (1 - l_discount)
		                    ELSE 0
		                    END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
		FROM
		  lineitem,
		  part
		WHERE
		  l_partkey = p_partkey
		  AND l_shipdate >= DATE '1995-09-01'
		  AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH

            """
        check {
            explainStr ->
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 6> sum(CASE WHEN <slot 14> LIKE 'PROMO%' THEN <slot 10> * (1 - <slot 11>) ELSE 0 END)), sum(<slot 7> sum(<slot 10> * (1 - <slot 11>)))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: sum(CASE WHEN <slot 14> LIKE 'PROMO%' THEN <slot 10> * (1 - <slot 11>) ELSE 0 END), sum(<slot 10> * (1 - <slot 11>))\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_partkey` = `p_partkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `p_partkey`") && 
		explainStr.contains("vec output tuple id: 4") && 
		explainStr.contains("output slot ids: 10 11 14 \n" + 
				"  |  hash output slot ids: 0 1 2 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `l_shipdate` >= '1995-09-01 00:00:00', `l_shipdate` < '1995-10-01 00:00:00'\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `l_partkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.part(part), PREAGGREGATION: ON") 
            
        }
    }
}
