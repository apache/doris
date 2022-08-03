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

suite("test_regression_test_tpcds_sf1_p1_q12", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  i_item_id
		, i_item_desc
		, i_category
		, i_class
		, i_current_price
		, sum(ws_ext_sales_price) itemrevenue
		, ((sum(ws_ext_sales_price) * 100) / sum(sum(ws_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
		FROM
		  web_sales
		, item
		, date_dim
		WHERE (ws_item_sk = i_item_sk)
		   AND (i_category IN ('Sports', 'Books', 'Home'))
		   AND (ws_sold_date_sk = d_date_sk)
		   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
		GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
		ORDER BY i_category ASC, i_class ASC, i_item_id ASC, i_item_desc ASC, revenueratio ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 18> <slot 13> `i_category` ASC, <slot 19> <slot 14> `i_class` ASC, <slot 20> <slot 11> `i_item_id` ASC, <slot 21> <slot 12> `i_item_desc` ASC, <slot 22> ((<slot 16> sum(`ws_ext_sales_price`) * 100) / <slot 17> sum(sum(`ws_ext_sales_price`)) OVER (PARTITION BY `i_class`)) ASC") && 
		explainStr.contains("order by: <slot 49> <slot 14> `i_class` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 16> sum(`ws_ext_sales_price`))\n" + 
				"  |  group by: <slot 11> `i_item_id`, <slot 12> `i_item_desc`, <slot 13> `i_category`, <slot 14> `i_class`, <slot 15> `i_current_price`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 34>)\n" + 
				"  |  group by: <slot 37>, <slot 38>, <slot 39>, <slot 40>, <slot 41>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 27> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 7") && 
		explainStr.contains("output slot ids: 34 37 38 39 40 41 \n" + 
				"  |  hash output slot ids: 32 25 28 29 30 31 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 6") && 
		explainStr.contains("output slot ids: 25 27 28 29 30 31 32 \n" + 
				"  |  hash output slot ids: 0 1 2 3 4 5 8 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ws_item_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: CAST(`d_date` AS DATE) >= '1999-02-22 00:00:00', CAST(`d_date` AS DATE) <= '1999-03-24 00:00:00'") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_category` IN ('Sports', 'Books', 'Home'))") 
            
        }
    }
}