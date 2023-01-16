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

suite("test_regression_test_tpcds_sf1_p1_q71", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  i_brand_id brand_id
		, i_brand brand
		, t_hour
		, t_minute
		, sum(ext_price) ext_price
		FROM
		  item
		, (
		   SELECT
		     ws_ext_sales_price ext_price
		   , ws_sold_date_sk sold_date_sk
		   , ws_item_sk sold_item_sk
		   , ws_sold_time_sk time_sk
		   FROM
		     web_sales
		   , date_dim
		   WHERE (d_date_sk = ws_sold_date_sk)
		      AND (d_moy = 11)
		      AND (d_year = 1999)
		UNION ALL    SELECT
		     cs_ext_sales_price ext_price
		   , cs_sold_date_sk sold_date_sk
		   , cs_item_sk sold_item_sk
		   , cs_sold_time_sk time_sk
		   FROM
		     catalog_sales
		   , date_dim
		   WHERE (d_date_sk = cs_sold_date_sk)
		      AND (d_moy = 11)
		      AND (d_year = 1999)
		UNION ALL    SELECT
		     ss_ext_sales_price ext_price
		   , ss_sold_date_sk sold_date_sk
		   , ss_item_sk sold_item_sk
		   , ss_sold_time_sk time_sk
		   FROM
		     store_sales
		   , date_dim
		   WHERE (d_date_sk = ss_sold_date_sk)
		      AND (d_moy = 11)
		      AND (d_year = 1999)
		)  tmp
		, time_dim
		WHERE (sold_item_sk = i_item_sk)
		   AND (i_manager_id = 1)
		   AND (time_sk = t_time_sk)
		   AND ((t_meal_time = 'breakfast')
		      OR (t_meal_time = 'dinner'))
		GROUP BY i_brand, i_brand_id, t_hour, t_minute
		ORDER BY ext_price DESC, i_brand_id ASC

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 42> <slot 41> sum(`ext_price`) DESC, <slot 43> <slot 38> `i_brand_id` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 41> sum(`ext_price`))\n" + 
				"  |  group by: <slot 37> `i_brand`, <slot 38> `i_brand_id`, <slot 39> `t_hour`, <slot 40> `t_minute`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 75>)\n" + 
				"  |  group by: <slot 79>, <slot 78>, <slot 82>, <slot 83>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 70> = `t_time_sk`)") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 75 78 79 82 83 \n" + 
				"  |  hash output slot ids: 32 68 71 72 31 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Src column hash been transformed by expr]\n" + 
				"  |  equal join conjunct: (<slot 23> `ws_item_sk` `cs_item_sk` `ss_item_sk` = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 68 70 71 72 \n" + 
				"  |  hash output slot ids: 21 24 29 30 ") && 
		explainStr.contains("TABLE: time_dim(time_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: ((`t_meal_time` = 'breakfast') OR (`t_meal_time` = 'dinner'))") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_manager_id` = 1)") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 61 62 63 64 65 66 67 \n" + 
				"  |  hash output slot ids: 16 17 18 19 20 14 15 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_moy` = 11), (`d_year` = 1999)") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 54 55 56 57 58 59 60 \n" + 
				"  |  hash output slot ids: 7 8 9 10 11 12 13 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `cs_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_moy` = 11), (`d_year` = 1999)") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 47 48 49 50 51 52 53 \n" + 
				"  |  hash output slot ids: 0 1 2 3 4 5 6 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ws_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_moy` = 11), (`d_year` = 1999)") 
            
        }
    }
}