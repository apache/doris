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

suite("test_regression_test_tpcds_sf1_p1_q62", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  substr(w_warehouse_name, 1, 20)
		, sm_type
		, web_name
		, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) <= 30) THEN 1 ELSE 0 END)) '30 days'
		, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 30)
		   AND ((ws_ship_date_sk - ws_sold_date_sk) <= 60) THEN 1 ELSE 0 END)) '31-60 days'
		, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 60)
		   AND ((ws_ship_date_sk - ws_sold_date_sk) <= 90) THEN 1 ELSE 0 END)) '61-90 days'
		, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 90)
		   AND ((ws_ship_date_sk - ws_sold_date_sk) <= 120) THEN 1 ELSE 0 END)) '91-120 days'
		, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 120) THEN 1 ELSE 0 END)) '>120 days'
		FROM
		  web_sales
		, warehouse
		, ship_mode
		, web_site
		, date_dim
		WHERE (d_month_seq BETWEEN 1200 AND (1200 + 11))
		   AND (ws_ship_date_sk = d_date_sk)
		   AND (ws_warehouse_sk = w_warehouse_sk)
		   AND (ws_ship_mode_sk = sm_ship_mode_sk)
		   AND (ws_web_site_sk = web_site_sk)
		GROUP BY substr(w_warehouse_name, 1, 20), sm_type, web_name
		ORDER BY substr(w_warehouse_name, 1, 20) ASC, sm_type ASC, web_name ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 21> <slot 13> substr(`w_warehouse_name`, 1, 20) ASC, <slot 22> <slot 14> `sm_type` ASC, <slot 23> <slot 15> `web_name` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 16> sum((CASE WHEN ((`ws_ship_date_sk` - `ws_sold_date_sk`) <= 30) THEN 1 ELSE 0 END))), sum(<slot 17> sum((CASE WHEN ((`ws_ship_date_sk` - `ws_sold_date_sk`) > 30) AND ((`ws_ship_date_sk` - `ws_sold_date_sk`) <= 60) THEN 1 ELSE 0 END))), sum(<slot 18> sum((CASE WHEN ((`ws_ship_date_sk` - `ws_sold_date_sk`) > 60) AND ((`ws_ship_date_sk` - `ws_sold_date_sk`) <= 90) THEN 1 ELSE 0 END))), sum(<slot 19> sum((CASE WHEN ((`ws_ship_date_sk` - `ws_sold_date_sk`) > 90) AND ((`ws_ship_date_sk` - `ws_sold_date_sk`) <= 120) THEN 1 ELSE 0 END))), sum(<slot 20> sum((CASE WHEN ((`ws_ship_date_sk` - `ws_sold_date_sk`) > 120) THEN 1 ELSE 0 END)))\n" + 
				"  |  group by: <slot 13> substr(`w_warehouse_name`, 1, 20), <slot 14> `sm_type`, <slot 15> `web_name`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((CASE WHEN ((<slot 56> - <slot 57>) <= 30) THEN 1 ELSE 0 END)), sum((CASE WHEN ((<slot 56> - <slot 57>) > 30) AND ((<slot 56> - <slot 57>) <= 60) THEN 1 ELSE 0 END)), sum((CASE WHEN ((<slot 56> - <slot 57>) > 60) AND ((<slot 56> - <slot 57>) <= 90) THEN 1 ELSE 0 END)), sum((CASE WHEN ((<slot 56> - <slot 57>) > 90) AND ((<slot 56> - <slot 57>) <= 120) THEN 1 ELSE 0 END)), sum((CASE WHEN ((<slot 56> - <slot 57>) > 120) THEN 1 ELSE 0 END))\n" + 
				"  |  group by: substr(<slot 63>, 1, 20), <slot 65>, <slot 67>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 49> = `web_site_sk`)") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 56 57 63 65 67 \n" + 
				"  |  hash output slot ids: 2 52 54 45 46 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 39> = `sm_ship_mode_sk`)") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 45 46 49 52 54 \n" + 
				"  |  hash output slot ids: 1 36 37 40 43 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 31> = `w_warehouse_sk`)") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 36 37 39 40 43 \n" + 
				"  |  hash output slot ids: 32 0 33 29 30 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_ship_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 7") && 
		explainStr.contains("output slot ids: 29 30 31 32 33 \n" + 
				"  |  hash output slot ids: 3 4 7 9 11 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ws_ship_date_sk`") && 
		explainStr.contains("TABLE: web_site(web_site), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: ship_mode(ship_mode), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: warehouse(warehouse), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_month_seq` >= 1200, `d_month_seq` <= 1211") 
            
        }
    }
}