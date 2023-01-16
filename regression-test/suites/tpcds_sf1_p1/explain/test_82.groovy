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

suite("test_regression_test_tpcds_sf1_p1_q82", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  i_item_id
		, i_item_desc
		, i_current_price
		FROM
		  item
		, inventory
		, date_dim
		, store_sales
		WHERE (i_current_price BETWEEN 62 AND (62 + 30))
		   AND (inv_item_sk = i_item_sk)
		   AND (d_date_sk = inv_date_sk)
		   AND (CAST(d_date AS DATE) BETWEEN CAST('2000-05-25' AS DATE) AND (CAST('2000-05-25' AS DATE) + INTERVAL  '60' DAY))
		   AND (i_manufact_id IN (129, 270, 821, 423))
		   AND (inv_quantity_on_hand BETWEEN 100 AND 500)
		   AND (ss_item_sk = i_item_sk)
		GROUP BY i_item_id, i_item_desc, i_current_price
		ORDER BY i_item_id ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 14> <slot 11> `i_item_id` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  group by: <slot 11> `i_item_id`, <slot 12> `i_item_desc`, <slot 13> `i_current_price`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  group by: <slot 38>, <slot 39>, <slot 40>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 31> = `ss_item_sk`)") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 38 39 40 \n" + 
				"  |  hash output slot ids: 28 29 30 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 18> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 7") && 
		explainStr.contains("output slot ids: 28 29 30 31 \n" + 
				"  |  hash output slot ids: 20 21 22 23 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`inv_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 6") && 
		explainStr.contains("output slot ids: 18 20 21 22 23 \n" + 
				"  |  hash output slot ids: 0 1 2 4 6 ") && 
		explainStr.contains("TABLE: inventory(inventory), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `inv_quantity_on_hand` >= 100, `inv_quantity_on_hand` <= 500\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `inv_item_sk`") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: CAST(`d_date` AS DATE) >= '2000-05-25 00:00:00', CAST(`d_date` AS DATE) <= '2000-07-24 00:00:00'") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `i_current_price` >= 62, `i_current_price` <= 92, (`i_manufact_id` IN (129, 270, 821, 423))") 
            
        }
    }
}