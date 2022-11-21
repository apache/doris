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

suite("test_explain_tpch_sf_1_q7") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  supp_nation,
		  cust_nation,
		  l_year,
		  sum(volume) AS revenue
		FROM (
		       SELECT
		         n1.n_name                          AS supp_nation,
		         n2.n_name                          AS cust_nation,
		         extract(YEAR FROM l_shipdate)      AS l_year,
		         l_extendedprice * (1 - l_discount) AS volume
		       FROM
		         supplier,
		         lineitem,
		         orders,
		         customer,
		         nation n1,
		         nation n2
		       WHERE
		         s_suppkey = l_suppkey
		         AND o_orderkey = l_orderkey
		         AND c_custkey = o_custkey
		         AND s_nationkey = n1.n_nationkey
		         AND c_nationkey = n2.n_nationkey
		         AND (
		           (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
		           OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
		         )
		         AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
		     ) AS shipping
		GROUP BY
		  supp_nation,
		  cust_nation,
		  l_year
		ORDER BY
		  supp_nation,
		  cust_nation,
		  l_year

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 23> <slot 19> `supp_nation` ASC, <slot 24> <slot 20> `cust_nation` ASC, <slot 25> <slot 21> `l_year` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 22> sum(<slot 68> * (1 - <slot 69>)))\n" + 
				"  |  group by: <slot 19> `supp_nation`, <slot 20> `cust_nation`, <slot 21> `l_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 68> * (1 - <slot 69>))\n" + 
				"  |  group by: <slot 76>, <slot 80>, year(<slot 67>)") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 66> = `n2`.`n_nationkey`\n" + 
				"  |  other predicates: ((<slot 113> = 'FRANCE' AND <slot 115> = 'GERMANY') OR (<slot 113> = 'GERMANY' AND <slot 115> = 'FRANCE'))\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `n2`.`n_nationkey`") && 
		explainStr.contains("other predicates: ((<slot 113> = 'FRANCE' AND <slot 115> = 'GERMANY') OR (<slot 113> = 'GERMANY' AND <slot 115> = 'FRANCE'))") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 67 68 69 76 80 \n" + 
				"  |  hash output slot ids: 1 54 55 56 63 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 51> = `c_custkey`\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `c_custkey`") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 54 55 56 63 66 \n" + 
				"  |  hash output slot ids: 52 43 44 45 13 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 40> = `n1`.`n_nationkey`\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `n1`.`n_nationkey`") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 43 44 45 51 52 \n" + 
				"  |  hash output slot ids: 0 34 35 36 42 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 31> = `o_orderkey`\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `o_orderkey`") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 34 35 36 40 42 \n" + 
				"  |  hash output slot ids: 33 10 27 28 29 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_suppkey` = `s_suppkey`\n" + 
				"  |  runtime filters: RF004[in_or_bloom] <- `s_suppkey`") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 27 28 29 31 33 \n" + 
				"  |  hash output slot ids: 2 3 4 8 11 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `l_shipdate` >= '1995-01-01 00:00:00', `l_shipdate` <= '1996-12-31 00:00:00'\n" + 
				"     runtime filters: RF003[in_or_bloom] -> <slot 8>, RF004[in_or_bloom] -> `l_suppkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.nation(nation), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`n2`.`n_name` = 'FRANCE' OR `n2`.`n_name` = 'GERMANY')") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.customer(customer), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> <slot 13>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.nation(nation), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`n1`.`n_name` = 'FRANCE' OR `n1`.`n_name` = 'GERMANY')") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.orders(orders), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> <slot 10>") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.supplier(supplier), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> <slot 11>")
            
        }
    }
}
