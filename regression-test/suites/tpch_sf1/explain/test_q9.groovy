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

suite("test_explain_tpch_sf_1_q9", "tpch_sf1") {
    explain {
            sql """
		SELECT
		  nation,
		  o_year,
		  sum(amount) AS sum_profit
		FROM (
		       SELECT
		         n_name                                                          AS nation,
		         extract(YEAR FROM o_orderdate)                                  AS o_year,
		         l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
		       FROM
		         part,
		         supplier,
		         lineitem,
		         partsupp,
		         orders,
		         nation
		       WHERE
		         s_suppkey = l_suppkey
		         AND ps_suppkey = l_suppkey
		         AND ps_partkey = l_partkey
		         AND p_partkey = l_partkey
		         AND o_orderkey = l_orderkey
		         AND s_nationkey = n_nationkey
		         AND p_name LIKE '%green%'
		     ) AS profit
		GROUP BY
		  nation,
		  o_year
		ORDER BY
		  nation,
		  o_year DESC

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 23> <slot 20> `nation` ASC, <slot 24> <slot 21> `o_year` DESC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 22> sum(`amount`))\n" + 
				"  |  group by: <slot 20> `nation`, <slot 21> `o_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(`l_extendedprice` * (1 - `l_discount`) - `ps_supplycost` * `l_quantity`)\n" + 
				"  |  group by: `n_name`, year(`o_orderdate`)") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: `s_nationkey` = `n_nationkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `n_nationkey`") && 
		explainStr.contains("output slot ids: 2 3 5 4 1 0 \n" + 
				"  |  hash output slot ids: 2 3 5 4 1 0 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_orderkey` = `o_orderkey`\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `o_orderkey`") && 
		explainStr.contains("output slot ids: 2 3 5 14 4 1 \n" + 
				"  |  hash output slot ids: 2 3 5 14 4 1 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_partkey` = `p_partkey`\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `p_partkey`") && 
		explainStr.contains("output slot ids: 2 3 5 13 14 4 \n" + 
				"  |  hash output slot ids: 2 3 5 13 14 4 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_suppkey` = `ps_suppkey`\n" + 
				"  |  equal join conjunct: `l_partkey` = `ps_partkey`\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `ps_suppkey`, RF004[in_or_bloom] <- `ps_partkey`") && 
		explainStr.contains("output slot ids: 2 3 5 10 13 14 4 \n" + 
				"  |  hash output slot ids: 2 3 5 10 13 14 4 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_suppkey` = `s_suppkey`\n" + 
				"  |  runtime filters: RF005[in_or_bloom] <- `s_suppkey`") && 
		explainStr.contains("output slot ids: 2 3 5 7 10 13 14 \n" + 
				"  |  hash output slot ids: 2 3 5 7 10 13 14 ") && 
		explainStr.contains("TABLE: lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `l_orderkey`, RF002[in_or_bloom] -> `l_partkey`, RF003[in_or_bloom] -> `l_suppkey`, RF004[in_or_bloom] -> `l_partkey`, RF005[in_or_bloom] -> `l_suppkey`") && 
		explainStr.contains("TABLE: nation(nation), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: orders(orders), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: part(part), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `p_name` LIKE '%green%'") && 
		explainStr.contains("TABLE: partsupp(partsupp), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: supplier(supplier), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `s_nationkey`") 
            
        }
    }
}