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

suite("test_regression_test_tpcds_sf1_p1_q94", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  count(DISTINCT ws_order_number) 'order count'
		, sum(ws_ext_ship_cost) 'total shipping cost'
		, sum(ws_net_profit) 'total net profit'
		FROM
		  web_sales ws1
		, date_dim
		, customer_address
		, web_site
		WHERE (d_date BETWEEN CAST('1999-2-01' AS DATE) AND (CAST('1999-2-01' AS DATE) + INTERVAL  '60' DAY))
		   AND (ws1.ws_ship_date_sk = d_date_sk)
		   AND (ws1.ws_ship_addr_sk = ca_address_sk)
		   AND (ca_state = 'IL')
		   AND (ws1.ws_web_site_sk = web_site_sk)
		   AND (web_company_name = 'pri')
		   AND (EXISTS (
		   SELECT *
		   FROM
		     web_sales ws2
		   WHERE (ws1.ws_order_number = ws2.ws_order_number)
		      AND (ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
		))
		   AND (NOT (EXISTS (
		   SELECT *
		   FROM
		     web_returns wr1
		   WHERE (ws1.ws_order_number = wr1.wr_order_number)
		)))
		ORDER BY count(DISTINCT ws_order_number) ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 141> <slot 138> count(<slot 132> `ws_order_number`) ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 135> count(<slot 132> `ws_order_number`)), sum(<slot 136> <slot 133> sum(`ws_ext_ship_cost`)), sum(<slot 137> <slot 134> sum(`ws_net_profit`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(<slot 132> `ws_order_number`), sum(<slot 133> sum(`ws_ext_ship_cost`)), sum(<slot 134> sum(`ws_net_profit`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (merge serialize)\n" + 
				"  |  output: sum(<slot 133> sum(`ws_ext_ship_cost`)), sum(<slot 134> sum(`ws_net_profit`))\n" + 
				"  |  group by: <slot 132> `ws_order_number`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 192>), sum(<slot 193>)\n" + 
				"  |  group by: <slot 191>") && 
		explainStr.contains("join op: LEFT ANTI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 178> = `wr1`.`wr_order_number`)") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 191 192 193 \n" + 
				"  |  hash output slot ids: 178 179 180 ") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 165> = `ws2`.`ws_order_number`)\n" + 
				"  |  other join predicates: (<slot 227> != <slot 232>)") && 
		explainStr.contains("other join predicates: (<slot 227> != <slot 232>)") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 178 179 180 \n" + 
				"  |  hash output slot ids: 1 164 165 166 167 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 159> = `web_site_sk`)") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 164 165 166 167 \n" + 
				"  |  hash output slot ids: 153 154 155 156 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 149> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 153 154 155 156 159 \n" + 
				"  |  hash output slot ids: 144 145 146 147 150 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws1`.`ws_ship_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 144 145 146 147 149 150 \n" + 
				"  |  hash output slot ids: 129 70 71 121 122 126 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ws1`.`ws_ship_date_sk`") && 
		explainStr.contains("TABLE: web_returns(web_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: web_site(web_site), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`web_company_name` = 'pri')") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ca_state` = 'IL')") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_date` >= '1999-02-01 00:00:00', `d_date` <= '1999-04-02 00:00:00'") 
            
        }
    }
}