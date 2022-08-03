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

suite("test_regression_test_tpcds_sf1_p1_q89", "tpch_sf1") {
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
		     i_category
		   , i_class
		   , i_brand
		   , s_store_name
		   , s_company_name
		   , d_moy
		   , sum(ss_sales_price) sum_sales
		   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name) avg_monthly_sales
		   FROM
		     item
		   , store_sales
		   , date_dim
		   , store
		   WHERE (ss_item_sk = i_item_sk)
		      AND (ss_sold_date_sk = d_date_sk)
		      AND (ss_store_sk = s_store_sk)
		      AND (d_year IN (1999))
		      AND (((i_category IN ('Books'         , 'Electronics'         , 'Sports'))
		            AND (i_class IN ('computers'         , 'stereo'         , 'football')))
		         OR ((i_category IN ('Men'         , 'Jewelry'         , 'Women'))
		            AND (i_class IN ('shirts'         , 'birdal'         , 'dresses'))))
		   GROUP BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy
		)  tmp1
		WHERE ((CASE WHEN (avg_monthly_sales <> 0) THEN (abs((sum_sales - avg_monthly_sales)) / avg_monthly_sales) ELSE null END) > CAST('0.1' AS DECIMAL))
		ORDER BY (sum_sales - avg_monthly_sales) ASC, s_store_name ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 30> (`sum_sales` - `avg_monthly_sales`) ASC, <slot 31> `s_store_name` ASC") && 
		explainStr.contains("predicates: ((CASE WHEN (<slot 72> != 0) THEN (abs((<slot 79> <slot 20> sum(`ss_sales_price`) - <slot 72>)) / <slot 72>) ELSE NULL END) > 0.1)") && 
		explainStr.contains("order by: <slot 73> <slot 14> `i_category` ASC, <slot 75> <slot 16> `i_brand` ASC, <slot 76> <slot 17> `s_store_name` ASC, <slot 77> <slot 18> `s_company_name` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 20> sum(`ss_sales_price`))\n" + 
				"  |  group by: <slot 14> `i_category`, <slot 15> `i_class`, <slot 16> `i_brand`, <slot 17> `s_store_name`, <slot 18> `s_company_name`, <slot 19> `d_moy`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 58>)\n" + 
				"  |  group by: <slot 62>, <slot 63>, <slot 64>, <slot 69>, <slot 70>, <slot 66>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 50> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 58 62 63 64 66 69 70 \n" + 
				"  |  hash output slot ids: 51 3 52 4 53 55 47 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 41> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 47 50 51 52 53 55 \n" + 
				"  |  hash output slot ids: 5 39 42 43 44 45 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 39 41 42 43 44 45 \n" + 
				"  |  hash output slot ids: 0 1 2 6 9 11 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_item_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` IN (1999))") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `i_category` IN ('Books', 'Electronics', 'Sports', 'Men', 'Jewelry', 'Women'), `i_class` IN ('computers', 'stereo', 'football', 'shirts', 'birdal', 'dresses'), (((`i_category` IN ('Books', 'Electronics', 'Sports')) AND (`i_class` IN ('computers', 'stereo', 'football'))) OR ((`i_category` IN ('Men', 'Jewelry', 'Women')) AND (`i_class` IN ('shirts', 'birdal', 'dresses'))))") 
            
        }
    }
}