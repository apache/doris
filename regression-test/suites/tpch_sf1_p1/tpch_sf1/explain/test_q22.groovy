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

suite("test_explain_tpch_sf_1_q22") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  cntrycode,
		  count(*)       AS numcust,
		  sum(c_acctbal) AS totacctbal
		FROM (
		       SELECT
		         substr(c_phone, 1, 2) AS cntrycode,
		         c_acctbal
		       FROM
		         customer
		       WHERE
		         substr(c_phone, 1, 2) IN
		         ('13', '31', '23', '29', '30', '18', '17')
		         AND c_acctbal > (
		           SELECT avg(c_acctbal)
		           FROM
		             customer
		           WHERE
		             c_acctbal > 0.00
		             AND substr(c_phone, 1, 2) IN
		                 ('13', '31', '23', '29', '30', '18', '17')
		         )
		         AND NOT exists(
		           SELECT *
		           FROM
		             orders
		           WHERE
		             o_custkey = c_custkey
		         )
		     ) AS custsale
		GROUP BY
		  cntrycode
		ORDER BY
		  cntrycode

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 32> <slot 29> `cntrycode` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 30> count(*)), sum(<slot 31> sum(<slot 37>))\n" + 
				"  |  group by: <slot 29> `cntrycode`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: count(*), sum(<slot 37>)\n" + 
				"  |  group by: substr(<slot 36>, 1, 2)") && 
		explainStr.contains("join op: LEFT ANTI JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `c_custkey` = `o_custkey`") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 36 37 \n" + 
				"  |  hash output slot ids: 25 26 ") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates: `c_acctbal` > <slot 3> avg(`c_acctbal`)") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.customer(customer), PREAGGREGATION: ON\n" + 
				"     PREDICATES: substr(`c_phone`, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.orders(orders), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: avg(<slot 2> avg(`c_acctbal`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: avg(`c_acctbal`)\n" + 
				"  |  group by: ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.customer(customer), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `c_acctbal` > 0.00, substr(`c_phone`, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')") 
            
        }
    }
}