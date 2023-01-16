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

suite("test_regression_test_tpcds_sf1_p1_q67", "regression_test_tpcds_sf1_p1") {
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
		   , i_product_name
		   , d_year
		   , d_qoy
		   , d_moy
		   , s_store_id
		   , sumsales
		   , rank() OVER (PARTITION BY i_category ORDER BY sumsales DESC) rk
		   FROM
		     (
		      SELECT
		        i_category
		      , i_class
		      , i_brand
		      , i_product_name
		      , d_year
		      , d_qoy
		      , d_moy
		      , s_store_id
		      , sum(COALESCE((ss_sales_price * ss_quantity), 0)) sumsales
		      FROM
		        store_sales
		      , date_dim
		      , store
		      , item
		      WHERE (ss_sold_date_sk = d_date_sk)
		         AND (ss_item_sk = i_item_sk)
		         AND (ss_store_sk = s_store_sk)
		         AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
		      GROUP BY ROLLUP (i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id)
		   )  dw1
		)  dw2
		WHERE (rk <= 100)
		ORDER BY i_category ASC, i_class ASC, i_brand ASC, i_product_name ASC, d_year ASC, d_qoy ASC, d_moy ASC, s_store_id ASC, sumsales ASC, rk ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 60> `i_category` ASC, <slot 61> `i_class` ASC, <slot 62> `i_brand` ASC, <slot 63> `i_product_name` ASC, <slot 64> `d_year` ASC, <slot 65> `d_qoy` ASC, <slot 66> `d_moy` ASC, <slot 67> `s_store_id` ASC, <slot 68> `sumsales` ASC, <slot 69> `rk` ASC") && 
		explainStr.contains("predicates: (<slot 112> <= 100)") && 
		explainStr.contains("order by: <slot 123> <slot 38> sum(coalesce((`ss_sales_price` * `ss_quantity`), 0)) DESC NULLS LAST") && 
		explainStr.contains("order by: <slot 124> `i_category` ASC, <slot 125> `sumsales` DESC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 38> sum(coalesce((`ss_sales_price` * `ss_quantity`), 0)))\n" + 
				"  |  group by: <slot 29> `i_category`, <slot 30> `i_class`, <slot 31> `i_brand`, <slot 32> `i_product_name`, <slot 33> `d_year`, <slot 34> `d_qoy`, <slot 35> `d_moy`, <slot 36> `s_store_id`, <slot 37> `GROUPING_ID`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(coalesce((<slot 26> `ss_sales_price` * <slot 27> `ss_quantity`), 0))\n" + 
				"  |  group by: <slot 18> `i_category`, <slot 19> `i_class`, <slot 20> `i_brand`, <slot 21> `i_product_name`, <slot 22> `d_year`, <slot 23> `d_qoy`, <slot 24> `d_moy`, <slot 25> `s_store_id`, <slot 28> `GROUPING_ID`") && 
		explainStr.contains("output slots: ``i_category``, ``i_class``, ``i_brand``, ``i_product_name``, ``d_year``, ``d_qoy``, ``d_moy``, ``s_store_id``, ``ss_sales_price``, ``ss_quantity``, ``GROUPING_ID``") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 84> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 95 96 97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 \n" + 
				"  |  hash output slot ids: 7 80 16 81 82 83 84 85 86 87 88 89 90 91 92 93 94 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 73> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 \n" + 
				"  |  hash output slot ids: 0 1 2 3 70 71 72 73 74 75 76 77 78 14 79 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 70 71 72 73 74 75 76 77 78 79 \n" + 
				"  |  hash output slot ids: 17 4 5 6 8 9 11 12 13 15 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_month_seq` >= 1200, `d_month_seq` <= 1211") 
            
        }
    }
}