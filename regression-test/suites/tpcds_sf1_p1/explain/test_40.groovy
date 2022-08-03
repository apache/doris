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

suite("test_regression_test_tpcds_sf1_p1_q40", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  w_state
		, i_item_id
		, sum((CASE WHEN (CAST(d_date AS DATE) < CAST('2000-03-11' AS DATE)) THEN (cs_sales_price - COALESCE(cr_refunded_cash, 0)) ELSE 0 END)) sales_before
		, sum((CASE WHEN (CAST(d_date AS DATE) >= CAST('2000-03-11' AS DATE)) THEN (cs_sales_price - COALESCE(cr_refunded_cash, 0)) ELSE 0 END)) sales_after
		FROM
		  catalog_sales
		LEFT JOIN catalog_returns ON (cs_order_number = cr_order_number)
		   AND (cs_item_sk = cr_item_sk)
		, warehouse
		, item
		, date_dim
		WHERE (i_current_price BETWEEN CAST('0.99' AS DECIMAL) AND CAST('1.49' AS DECIMAL))
		   AND (i_item_sk = cs_item_sk)
		   AND (cs_warehouse_sk = w_warehouse_sk)
		   AND (cs_sold_date_sk = d_date_sk)
		   AND (CAST(d_date AS DATE) BETWEEN (CAST('2000-03-11' AS DATE) - INTERVAL  '30' DAY) AND (CAST('2000-03-11' AS DATE) + INTERVAL  '30' DAY))
		GROUP BY w_state, i_item_id
		ORDER BY w_state ASC, i_item_id ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 19> <slot 15> `w_state` ASC, <slot 20> <slot 16> `i_item_id` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 17> sum((CASE WHEN (CAST(`d_date` AS DATE) < '2000-03-11 00:00:00') THEN (`cs_sales_price` - coalesce(`cr_refunded_cash`, 0)) ELSE 0 END))), sum(<slot 18> sum((CASE WHEN (CAST(`d_date` AS DATE) >= '2000-03-11 00:00:00') THEN (`cs_sales_price` - coalesce(`cr_refunded_cash`, 0)) ELSE 0 END)))\n" + 
				"  |  group by: <slot 15> `w_state`, <slot 16> `i_item_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((CASE WHEN (CAST(<slot 62> AS DATE) < '2000-03-11 00:00:00') THEN (<slot 54> - coalesce(<slot 66>, 0)) ELSE 0 END)), sum((CASE WHEN (CAST(<slot 62> AS DATE) >= '2000-03-11 00:00:00') THEN (<slot 54> - coalesce(<slot 66>, 0)) ELSE 0 END))\n" + 
				"  |  group by: <slot 57>, <slot 59>") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 40> = `cr_order_number`)\n" + 
				"  |  equal join conjunct: (<slot 41> = `cr_item_sk`)") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 54 57 59 62 66 \n" + 
				"  |  hash output slot ids: 50 8 42 45 47 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 34> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 40 41 42 45 47 50 \n" + 
				"  |  hash output slot ids: 32 35 37 6 30 31 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 24> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 30 31 32 34 35 37 \n" + 
				"  |  hash output slot ids: 5 23 24 25 27 28 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_warehouse_sk` = `w_warehouse_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `w_warehouse_sk`") && 
		explainStr.contains("vec output tuple id: 7") && 
		explainStr.contains("output slot ids: 23 24 25 27 28 \n" + 
				"  |  hash output slot ids: 0 2 4 7 13 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `cs_warehouse_sk`") && 
		explainStr.contains("TABLE: catalog_returns(catalog_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: CAST(`d_date` AS DATE) >= '2000-02-10 00:00:00', CAST(`d_date` AS DATE) <= '2000-04-10 00:00:00'") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `i_current_price` >= 0.99, `i_current_price` <= 1.49") && 
		explainStr.contains("TABLE: warehouse(warehouse), PREAGGREGATION: ON") 
            
        }
    }
}