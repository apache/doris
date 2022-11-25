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

suite("test_explain_tpch_sf_1_q18") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  c_name,
		  c_custkey,
		  o_orderkey,
		  o_orderdate,
		  o_totalprice,
		  sum(l_quantity)
		FROM
		  customer,
		  orders,
		  lineitem
		WHERE
		  o_orderkey IN (
		    SELECT l_orderkey
		    FROM
		      lineitem
		    GROUP BY
		      l_orderkey
		    HAVING
		      sum(l_quantity) > 300
		  )
		  AND c_custkey = o_custkey
		  AND o_orderkey = l_orderkey
		GROUP BY
		  c_name,
		  c_custkey,
		  o_orderkey,
		  o_orderdate,
		  o_totalprice
		ORDER BY
		  o_totalprice DESC,
		  o_orderdate
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 24> <slot 22> `o_totalprice` DESC, <slot 25> <slot 21> `o_orderdate` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 23> sum(<slot 53>))\n" + 
				"  |  group by: <slot 18> `c_name`, <slot 19> `c_custkey`, <slot 20> `o_orderkey`, <slot 21> `o_orderdate`, <slot 22> `o_totalprice`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 53>)\n" + 
				"  |  group by: <slot 58>, <slot 59>, <slot 54>, <slot 55>, <slot 56>") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 44> = <slot 8> `l_orderkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- <slot 8> `l_orderkey`") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 53 54 55 56 58 59 \n" + 
				"  |  hash output slot ids: 48 50 51 45 46 47 ") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 38> = <slot 2> `l_orderkey`\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- <slot 2> `l_orderkey`") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 44 45 46 47 48 50 51 \n" + 
				"  |  hash output slot ids: 36 37 38 39 40 42 43 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 35> = `c_custkey`\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `c_custkey`") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 36 37 38 39 40 42 43 \n" + 
				"  |  hash output slot ids: 32 33 34 12 13 30 31 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_orderkey` = `o_orderkey`\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `o_orderkey`") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 30 31 32 33 34 35 \n" + 
				"  |  hash output slot ids: 16 17 5 11 14 15 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> <slot 11>, RF003[in_or_bloom] -> `l_orderkey`") && 
		explainStr.contains("VAGGREGATE (update finalize)\n" + 
				"  |  output: sum(`l_quantity`)\n" + 
				"  |  group by: `l_orderkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.lineitem(lineitem), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (update finalize)\n" + 
				"  |  output: sum(`l_quantity`)\n" + 
				"  |  group by: `l_orderkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.lineitem(lineitem), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.orders(orders), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> <slot 5>, RF002[in_or_bloom] -> <slot 17>")
            
        }
    }
}
