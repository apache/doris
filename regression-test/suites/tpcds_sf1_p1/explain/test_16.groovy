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

suite("test_regression_test_tpcds_sf1_p1_q16", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  count(DISTINCT cs_order_number) 'order count'
		, sum(cs_ext_ship_cost) 'total shipping cost'
		, sum(cs_net_profit) 'total net profit'
		FROM
		  catalog_sales cs1
		, date_dim
		, customer_address
		, call_center
		WHERE (d_date BETWEEN CAST('2002-2-01' AS DATE) AND (CAST('2002-2-01' AS DATE) + INTERVAL  '60' DAY))
		   AND (cs1.cs_ship_date_sk = d_date_sk)
		   AND (cs1.cs_ship_addr_sk = ca_address_sk)
		   AND (ca_state = 'GA')
		   AND (cs1.cs_call_center_sk = cc_call_center_sk)
		   AND (cc_county IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County'))
		   AND (EXISTS (
		   SELECT *
		   FROM
		     catalog_sales cs2
		   WHERE (cs1.cs_order_number = cs2.cs_order_number)
		      AND (cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
		))
		   AND (NOT (EXISTS (
		   SELECT *
		   FROM
		     catalog_returns cr1
		   WHERE (cs1.cs_order_number = cr1.cr_order_number)
		)))
		ORDER BY count(DISTINCT cs_order_number) ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 147> <slot 144> count(<slot 138> `cs_order_number`) ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 141> count(<slot 138> `cs_order_number`)), sum(<slot 142> <slot 139> sum(`cs_ext_ship_cost`)), sum(<slot 143> <slot 140> sum(`cs_net_profit`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(<slot 138> `cs_order_number`), sum(<slot 139> sum(`cs_ext_ship_cost`)), sum(<slot 140> sum(`cs_net_profit`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (merge serialize)\n" + 
				"  |  output: sum(<slot 139> sum(`cs_ext_ship_cost`)), sum(<slot 140> sum(`cs_net_profit`))\n" + 
				"  |  group by: <slot 138> `cs_order_number`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 198>), sum(<slot 199>)\n" + 
				"  |  group by: <slot 197>") && 
		explainStr.contains("join op: LEFT ANTI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 184> = `cr1`.`cr_order_number`)") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 197 198 199 \n" + 
				"  |  hash output slot ids: 184 185 186 ") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 171> = `cs2`.`cs_order_number`)\n" + 
				"  |  other join predicates: (<slot 233> != <slot 238>)") && 
		explainStr.contains("other join predicates: (<slot 233> != <slot 238>)") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 184 185 186 \n" + 
				"  |  hash output slot ids: 1 170 171 172 173 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 165> = `cc_call_center_sk`)") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 170 171 172 173 \n" + 
				"  |  hash output slot ids: 160 161 162 159 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 155> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 159 160 161 162 165 \n" + 
				"  |  hash output slot ids: 150 151 152 153 156 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs1`.`cs_ship_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 150 151 152 153 155 156 \n" + 
				"  |  hash output slot ids: 128 132 70 71 135 127 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `cs1`.`cs_ship_date_sk`") && 
		explainStr.contains("TABLE: catalog_returns(catalog_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: call_center(call_center), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`cc_county` IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County'))") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ca_state` = 'GA')") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_date` >= '2002-02-01 00:00:00', `d_date` <= '2002-04-02 00:00:00'") 
            
        }
    }
}