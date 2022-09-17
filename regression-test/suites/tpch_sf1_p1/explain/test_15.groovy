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

suite("test_explain_tpch_sf_1_q15", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  s_suppkey,
		  s_name,
		  s_address,
		  s_phone,
		  total_revenue
		FROM
		  supplier,
		  revenue1
		WHERE
		  s_suppkey = supplier_no
		  AND total_revenue = (
		    SELECT max(total_revenue)
		    FROM
		      revenue1
		  )
		ORDER BY
		  s_suppkey;

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 23> `s_suppkey` ASC") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 36> = <slot 17> max(<slot 13> sum(`l_extendedprice` * (1 - `l_discount`)))") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 38 39 40 41 44 \n" + 
				"  |  hash output slot ids: 32 33 36 30 31 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `s_suppkey` = <slot 4> `l_suppkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- <slot 4> `l_suppkey`") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 30 31 32 33 36 \n" + 
				"  |  hash output slot ids: 19 20 21 5 22 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.supplier(supplier), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts. No AggregateInfo\n" + 
				"     PREDICATES: `default_cluster:regression_test_tpch_sf1_p1.supplier`.`__DORIS_DELETE_SIGN__` = 0\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `s_suppkey`") && 
		explainStr.contains("VAGGREGATE (update finalize)\n" + 
				"  |  output: max(<slot 13> sum(`l_extendedprice` * (1 - `l_discount`)))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update finalize)\n" + 
				"  |  output: sum(`l_extendedprice` * (1 - `l_discount`))\n" + 
				"  |  group by: `l_suppkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.lineitem(lineitem), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts. conjunct on `L_SHIPDATE` which is StorageEngine value column\n" + 
				"     PREDICATES: `l_shipdate` >= '1996-01-01 00:00:00', `l_shipdate` < '1996-04-01 00:00:00', `default_cluster:regression_test_tpch_sf1_p1.lineitem`.`__DORIS_DELETE_SIGN__` = 0") && 
		explainStr.contains("VAGGREGATE (update finalize)\n" + 
				"  |  output: sum(`l_extendedprice` * (1 - `l_discount`))\n" + 
				"  |  group by: `l_suppkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.lineitem(lineitem), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts. conjunct on `L_SHIPDATE` which is StorageEngine value column\n" + 
				"     PREDICATES: `l_shipdate` >= '1996-01-01 00:00:00', `l_shipdate` < '1996-04-01 00:00:00', `default_cluster:regression_test_tpch_sf1_p1.lineitem`.`__DORIS_DELETE_SIGN__` = 0") 
            
        }
    }
}