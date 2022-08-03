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

suite("test_regression_test_tpcds_sf1_p1_q21", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT *
		FROM
		  (
		   SELECT
		     w_warehouse_name
		   , i_item_id
		   , sum((CASE WHEN (CAST(d_date AS DATE) < CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_before
		   , sum((CASE WHEN (CAST(d_date AS DATE) >= CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_after
		   FROM
		     inventory
		   , warehouse
		   , item
		   , date_dim
		   WHERE (i_current_price BETWEEN CAST('0.99' AS DECIMAL) AND CAST('1.49' AS DECIMAL))
		      AND (i_item_sk = inv_item_sk)
		      AND (inv_warehouse_sk = w_warehouse_sk)
		      AND (inv_date_sk = d_date_sk)
		      AND (d_date BETWEEN (CAST('2000-03-11' AS DATE) - INTERVAL  '30' DAY) AND (CAST('2000-03-11' AS DATE) + INTERVAL  '30' DAY))
		   GROUP BY w_warehouse_name, i_item_id
		)  x
		WHERE ((CASE WHEN (inv_before > 0) THEN (CAST(inv_after AS DECIMAL(7,2)) / inv_before) ELSE null END) BETWEEN (CAST('2.00' AS DECIMAL) / CAST('3.00' AS DECIMAL)) AND (CAST('3.00' AS DECIMAL) / CAST('2.00' AS DECIMAL)))
		ORDER BY w_warehouse_name ASC, i_item_id ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 19> `w_warehouse_name` ASC, <slot 20> `i_item_id` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 13> sum((CASE WHEN (CAST(`d_date` AS DATE) < '2000-03-11 00:00:00') THEN `inv_quantity_on_hand` ELSE 0 END))), sum(<slot 14> sum((CASE WHEN (CAST(`d_date` AS DATE) >= '2000-03-11 00:00:00') THEN `inv_quantity_on_hand` ELSE 0 END)))\n" + 
				"  |  group by: <slot 11> `w_warehouse_name`, <slot 12> `i_item_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((CASE WHEN (CAST(<slot 48> AS DATE) < '2000-03-11 00:00:00') THEN <slot 39> ELSE 0 END)), sum((CASE WHEN (CAST(<slot 48> AS DATE) >= '2000-03-11 00:00:00') THEN <slot 39> ELSE 0 END))\n" + 
				"  |  group by: <slot 46>, <slot 43>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 33> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 39 43 46 48 \n" + 
				"  |  hash output slot ids: 34 2 37 30 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 25> = `w_warehouse_sk`)") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 30 33 34 37 \n" + 
				"  |  hash output slot ids: 0 23 26 27 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`inv_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 7") && 
		explainStr.contains("output slot ids: 23 25 26 27 \n" + 
				"  |  hash output slot ids: 1 3 7 9 ") && 
		explainStr.contains("TABLE: inventory(inventory), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `inv_item_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_date` >= '2000-02-10 00:00:00', `d_date` <= '2000-04-10 00:00:00'") && 
		explainStr.contains("TABLE: warehouse(warehouse), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `i_current_price` >= 0.99, `i_current_price` <= 1.49") 
            
        }
    }
}