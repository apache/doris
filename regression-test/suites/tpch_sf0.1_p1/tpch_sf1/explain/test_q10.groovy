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

suite("test_explain_tpch_sf_1_q10") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  c_custkey,
		  c_name,
		  sum(l_extendedprice * (1 - l_discount)) AS revenue,
		  c_acctbal,
		  n_name,
		  c_address,
		  c_phone,
		  c_comment
		FROM
		  customer,
		  orders,
		  lineitem,
		  nation
		WHERE
		  c_custkey = o_custkey
		  AND l_orderkey = o_orderkey
		  AND o_orderdate >= DATE '1993-10-01'
		  AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH
		  AND l_returnflag = 'R'
		  AND c_nationkey = n_nationkey
		GROUP BY
		  c_custkey,
		  c_name,
		  c_acctbal,
		  c_phone,
		  n_name,
		  c_address,
		  c_comment
		ORDER BY
		  revenue DESC
		LIMIT 20

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 24> <slot 23> sum(<slot 53> * (1 - <slot 54>)) DESC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 23> sum(<slot 53> * (1 - <slot 54>)))\n" + 
				"  |  group by: <slot 16> `c_custkey`, <slot 17> `c_name`, <slot 18> `c_acctbal`, <slot 19> `c_phone`, <slot 20> `n_name`, <slot 21> `c_address`, <slot 22> `c_comment`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 53> * (1 - <slot 54>))\n" + 
				"  |  group by: <slot 60>, <slot 61>, <slot 62>, <slot 64>, <slot 67>, <slot 63>, <slot 65>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 52> = `n_nationkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `n_nationkey`") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 53 54 60 61 62 63 64 65 67 \n" + 
				"  |  hash output slot ids: 48 49 50 51 5 39 40 46 47 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 36> = `c_custkey`\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `c_custkey`") && 
		explainStr.contains("vec output tuple id: 7") && 
		explainStr.contains("output slot ids: 39 40 46 47 48 49 50 51 52 \n" + 
				"  |  hash output slot ids: 32 0 33 1 4 6 7 8 14 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_orderkey` = `o_orderkey`\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `o_orderkey`") && 
		explainStr.contains("vec output tuple id: 6") && 
		explainStr.contains("output slot ids: 32 33 36 \n" + 
				"  |  hash output slot ids: 2 3 9 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `l_returnflag` = 'R'\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `l_orderkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.nation(nation), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.customer(customer), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> <slot 14>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.orders(orders), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `o_orderdate` >= '1993-10-01 00:00:00', `o_orderdate` < '1994-01-01 00:00:00'\n" + 
				"     runtime filters: RF001[in_or_bloom] -> <slot 9>")
            
        }
    }
}
