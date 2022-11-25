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

suite("test_explain_tpch_sf_1_q19") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue
		FROM
		  lineitem,
		  part
		WHERE
		  (
		    p_partkey = l_partkey
		    AND p_brand = 'Brand#12'
		    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		    AND l_quantity >= 1 AND l_quantity <= 1 + 10
		    AND p_size BETWEEN 1 AND 5
		    AND l_shipmode IN ('AIR', 'AIR REG')
		    AND l_shipinstruct = 'DELIVER IN PERSON'
		  )
		  OR
		  (
		    p_partkey = l_partkey
		    AND p_brand = 'Brand#23'
		    AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		    AND l_quantity >= 10 AND l_quantity <= 10 + 10
		    AND p_size BETWEEN 1 AND 10
		    AND l_shipmode IN ('AIR', 'AIR REG')
		    AND l_shipinstruct = 'DELIVER IN PERSON'
		  )
		  OR
		  (
		    p_partkey = l_partkey
		    AND p_brand = 'Brand#34'
		    AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		    AND l_quantity >= 20 AND l_quantity <= 20 + 10
		    AND p_size BETWEEN 1 AND 15
		    AND l_shipmode IN ('AIR', 'AIR REG')
		    AND l_shipinstruct = 'DELIVER IN PERSON'
		  )

            """
        check {
            explainStr ->
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 10> sum(<slot 12> * (1 - <slot 13>)))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: sum(<slot 12> * (1 - <slot 13>))\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_partkey` = `p_partkey`\n" + 
				"  |  other predicates: ((<slot 30> = 'Brand#12' AND <slot 31> IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND <slot 27> >= 1 AND <slot 27> <= 11 AND <slot 29> <= 5) OR (<slot 30> = 'Brand#23' AND <slot 31> IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND <slot 27> >= 10 AND <slot 27> <= 20 AND <slot 29> <= 10) OR (<slot 30> = 'Brand#34' AND <slot 31> IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND <slot 27> >= 20 AND <slot 27> <= 30 AND <slot 29> <= 15))") && 
		explainStr.contains("other predicates: ((<slot 30> = 'Brand#12' AND <slot 31> IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND <slot 27> >= 1 AND <slot 27> <= 11 AND <slot 29> <= 5) OR (<slot 30> = 'Brand#23' AND <slot 31> IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND <slot 27> >= 10 AND <slot 27> <= 20 AND <slot 29> <= 10) OR (<slot 30> = 'Brand#34' AND <slot 31> IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND <slot 27> >= 20 AND <slot 27> <= 30 AND <slot 29> <= 15))\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `p_partkey`") && 
		explainStr.contains("vec output tuple id: 4") && 
		explainStr.contains("output slot ids: 12 13 \n" + 
				"  |  hash output slot ids: 0 1 4 7 8 9 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `l_shipmode` IN ('AIR', 'AIR REG'), `l_shipinstruct` = 'DELIVER IN PERSON', `l_quantity` >= 1, `l_quantity` <= 30\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `l_partkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.part(part), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `p_size` >= 1, (`p_brand` = 'Brand#12' OR `p_brand` = 'Brand#23' OR `p_brand` = 'Brand#34'), `p_size` <= 15, `p_container` IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG', 'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK', 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')") 
            
        }
    }
}
