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

suite("test_regression_test_tpcds_sf1_p1_q95", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  ws_wh AS (
		   SELECT
		     ws1.ws_order_number
		   , ws1.ws_warehouse_sk wh1
		   , ws2.ws_warehouse_sk wh2
		   FROM
		     web_sales ws1
		   , web_sales ws2
		   WHERE (ws1.ws_order_number = ws2.ws_order_number)
		      AND (ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
		)
		SELECT
		  count(DISTINCT ws_order_number) 'order count'
		, sum(ws_ext_ship_cost) 'total shipping cost'
		, sum(ws_net_profit) 'total net profit'
		FROM
		  web_sales ws1
		, date_dim
		, customer_address
		, web_site
		WHERE (CAST(d_date AS DATE) BETWEEN CAST('1999-2-01' AS DATE) AND (CAST('1999-2-01' AS DATE) + INTERVAL  '60' DAY))
		   AND (ws1.ws_ship_date_sk = d_date_sk)
		   AND (ws1.ws_ship_addr_sk = ca_address_sk)
		   AND (ca_state = 'IL')
		   AND (ws1.ws_web_site_sk = web_site_sk)
		   AND (web_company_name = 'pri')
		   AND (ws1.ws_order_number IN (
		   SELECT ws_order_number
		   FROM
		     ws_wh
		))
		   AND (ws1.ws_order_number IN (
		   SELECT wr_order_number
		   FROM
		     web_returns
		   , ws_wh
		   WHERE (wr_order_number = ws_wh.ws_order_number)
		))
		ORDER BY count(DISTINCT ws_order_number) ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 38> <slot 35> count(<slot 29> `ws_order_number`) ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 32> count(<slot 29> `ws_order_number`)), sum(<slot 33> <slot 30> sum(`ws_ext_ship_cost`)), sum(<slot 34> <slot 31> sum(`ws_net_profit`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(<slot 29> `ws_order_number`), sum(<slot 30> sum(`ws_ext_ship_cost`)), sum(<slot 31> sum(`ws_net_profit`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (merge serialize)\n" + 
				"  |  output: sum(<slot 30> sum(`ws_ext_ship_cost`)), sum(<slot 31> sum(`ws_net_profit`))\n" + 
				"  |  group by: <slot 29> `ws_order_number`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 97>), sum(<slot 98>)\n" + 
				"  |  group by: <slot 96>") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 75> = <slot 91>") && 
		explainStr.contains("vec output tuple id: 24") && 
		explainStr.contains("output slot ids: 96 97 98 \n" + 
				"  |  hash output slot ids: 75 76 77 ") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 59> = <slot 71>") && 
		explainStr.contains("vec output tuple id: 21") && 
		explainStr.contains("output slot ids: 75 76 77 \n" + 
				"  |  hash output slot ids: 59 60 61 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 54> = `web_site_sk`)") && 
		explainStr.contains("vec output tuple id: 19") && 
		explainStr.contains("output slot ids: 59 60 61 \n" + 
				"  |  hash output slot ids: 49 50 51 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 45> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 18") && 
		explainStr.contains("output slot ids: 49 50 51 54 \n" + 
				"  |  hash output slot ids: 41 42 43 46 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws1`.`ws_ship_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 17") && 
		explainStr.contains("output slot ids: 41 42 43 45 46 \n" + 
				"  |  hash output slot ids: 18 19 23 8 26 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ws1`.`ws_ship_date_sk`") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (`wr_order_number` = <slot 87>)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- <slot 87>") && 
		explainStr.contains("vec output tuple id: 23") && 
		explainStr.contains("output slot ids: 91 \n" + 
				"  |  hash output slot ids: 16 ") && 
		explainStr.contains("TABLE: web_returns(web_returns), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `wr_order_number`") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Inconsistent distribution of table and queries]\n" + 
				"  |  equal join conjunct: (`ws1`.`ws_order_number` = `ws2`.`ws_order_number`)\n" + 
				"  |  other predicates: (<slot 137> != <slot 138>)") && 
		explainStr.contains("other predicates: (<slot 137> != <slot 138>)\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `ws2`.`ws_order_number`") && 
		explainStr.contains("vec output tuple id: 22") && 
		explainStr.contains("output slot ids: 87 \n" + 
				"  |  hash output slot ids: 9 10 11 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF003[in_or_bloom] -> `ws1`.`ws_order_number`") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Inconsistent distribution of table and queries]\n" + 
				"  |  equal join conjunct: (`ws1`.`ws_order_number` = `ws2`.`ws_order_number`)\n" + 
				"  |  other predicates: (<slot 130> != <slot 131>)") && 
		explainStr.contains("other predicates: (<slot 130> != <slot 131>)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `ws2`.`ws_order_number`") && 
		explainStr.contains("vec output tuple id: 20") && 
		explainStr.contains("output slot ids: 71 \n" + 
				"  |  hash output slot ids: 0 1 2 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `ws1`.`ws_order_number`") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: web_site(web_site), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`web_company_name` = 'pri')") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ca_state` = 'IL')") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: CAST(`d_date` AS DATE) >= '1999-02-01 00:00:00', CAST(`d_date` AS DATE) <= '1999-04-02 00:00:00'") 
            
        }
    }
}