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

suite("test_regression_test_tpcds_sf1_p1_q55", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  i_brand_id brand_id
		, i_brand brand
		, sum(ss_ext_sales_price) ext_price
		FROM
		  date_dim
		, store_sales
		, item
		WHERE (d_date_sk = ss_sold_date_sk)
		   AND (ss_item_sk = i_item_sk)
		   AND (i_manager_id = 28)
		   AND (d_moy = 11)
		   AND (d_year = 1999)
		GROUP BY i_brand, i_brand_id
		ORDER BY ext_price DESC, i_brand_id ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 13> <slot 12> sum(`ss_ext_sales_price`) DESC, <slot 14> <slot 11> `i_brand_id` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 12> sum(`ss_ext_sales_price`))\n" + 
				"  |  group by: <slot 10> `i_brand`, <slot 11> `i_brand_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 22>)\n" + 
				"  |  group by: <slot 29>, <slot 28>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 18> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 6") && 
		explainStr.contains("output slot ids: 22 28 29 \n" + 
				"  |  hash output slot ids: 16 0 1 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 5") && 
		explainStr.contains("output slot ids: 16 18 \n" + 
				"  |  hash output slot ids: 2 5 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_manager_id` = 28)") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_moy` = 11), (`d_year` = 1999)") 
            
        }
    }
}