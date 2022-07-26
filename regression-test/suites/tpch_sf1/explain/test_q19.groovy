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

suite("test_explain_tpch_sf_1_q19", "tpch_sf1") {
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
				"  |  output: sum(<slot 10> sum(`l_extendedprice` * (1 - `l_discount`)))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: sum(`l_extendedprice` * (1 - `l_discount`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_partkey` = `p_partkey`\n" + 
				"  |  other predicates: ((`p_brand` = 'Brand#12' AND `p_container` IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND `l_quantity` >= 1 AND `l_quantity` <= 11 AND `p_size` <= 5) OR (`p_brand` = 'Brand#23' AND `p_container` IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND `l_quantity` >= 10 AND `l_quantity` <= 20 AND `p_size` <= 10) OR (`p_brand` = 'Brand#34' AND `p_container` IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND `l_quantity` >= 20 AND `l_quantity` <= 30 AND `p_size` <= 15))") && 
		explainStr.contains("other predicates: ((`p_brand` = 'Brand#12' AND `p_container` IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND `l_quantity` >= 1 AND `l_quantity` <= 11 AND `p_size` <= 5) OR (`p_brand` = 'Brand#23' AND `p_container` IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND `l_quantity` >= 10 AND `l_quantity` <= 20 AND `p_size` <= 10) OR (`p_brand` = 'Brand#34' AND `p_container` IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND `l_quantity` >= 20 AND `l_quantity` <= 30 AND `p_size` <= 15))\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `p_partkey`") && 
		explainStr.contains("output slot ids: 0 1 \n" + 
				"  |  hash output slot ids: 0 1 7 9 8 4 ") && 
		explainStr.contains("TABLE: lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `l_shipmode` IN ('AIR', 'AIR REG'), `l_shipinstruct` = 'DELIVER IN PERSON', `l_quantity` >= 1, `l_quantity` <= 30\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `l_partkey`") && 
		explainStr.contains("TABLE: part(part), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `p_size` >= 1, (`p_brand` = 'Brand#12' OR `p_brand` = 'Brand#23' OR `p_brand` = 'Brand#34'), `p_container` IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG', 'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK', 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')") 
            
        }
    }
}