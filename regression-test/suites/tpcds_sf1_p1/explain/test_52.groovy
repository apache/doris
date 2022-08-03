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

suite("test_regression_test_tpcds_sf1_p1_q52", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  dt.d_year
		, item.i_brand_id brand_id
		, item.i_brand brand
		, sum(ss_ext_sales_price) ext_price
		FROM
		  date_dim dt
		, store_sales
		, item
		WHERE (dt.d_date_sk = store_sales.ss_sold_date_sk)
		   AND (store_sales.ss_item_sk = item.i_item_sk)
		   AND (item.i_manager_id = 1)
		   AND (dt.d_moy = 11)
		   AND (dt.d_year = 2000)
		GROUP BY dt.d_year, item.i_brand, item.i_brand_id
		ORDER BY dt.d_year ASC, ext_price DESC, brand_id ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 14> <slot 10> `dt`.`d_year` ASC, <slot 15> <slot 13> sum(`ss_ext_sales_price`) DESC, <slot 16> <slot 12> `item`.`i_brand_id` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 13> sum(`ss_ext_sales_price`))\n" + 
				"  |  group by: <slot 10> `dt`.`d_year`, <slot 11> `item`.`i_brand`, <slot 12> `item`.`i_brand_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 24>)\n" + 
				"  |  group by: <slot 27>, <slot 31>, <slot 30>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 20> = `item`.`i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 6") && 
		explainStr.contains("output slot ids: 24 27 30 31 \n" + 
				"  |  hash output slot ids: 1 18 2 21 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`store_sales`.`ss_sold_date_sk` = `dt`.`d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `dt`.`d_date_sk`") && 
		explainStr.contains("vec output tuple id: 5") && 
		explainStr.contains("output slot ids: 18 20 21 \n" + 
				"  |  hash output slot ids: 0 3 6 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `store_sales`.`ss_sold_date_sk`") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`item`.`i_manager_id` = 1)") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`dt`.`d_moy` = 11), (`dt`.`d_year` = 2000)") 
            
        }
    }
}