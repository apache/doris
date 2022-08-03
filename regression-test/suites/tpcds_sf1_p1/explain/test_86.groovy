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

suite("test_regression_test_tpcds_sf1_p1_q86", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  sum(ws_net_paid) total_sum
		, i_category
		, i_class
		, (GROUPING (i_category) + GROUPING (i_class)) lochierarchy
		, rank() OVER (PARTITION BY (GROUPING (i_category) + GROUPING (i_class)), (CASE WHEN (GROUPING (i_class) = 0) THEN i_category END) ORDER BY sum(ws_net_paid) DESC) rank_within_parent
		FROM
		  web_sales
		, date_dim d1
		, item
		WHERE (d1.d_month_seq BETWEEN 1200 AND (1200 + 11))
		   AND (d1.d_date_sk = ws_sold_date_sk)
		   AND (i_item_sk = ws_item_sk)
		GROUP BY ROLLUP (i_category, i_class)
		ORDER BY lochierarchy DESC, (CASE WHEN (lochierarchy = 0) THEN i_category END) ASC, rank_within_parent ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 25> (grouping(<slot 20> `GROUPING_PREFIX_`i_category``) + grouping(<slot 21> `GROUPING_PREFIX_`i_class``)) DESC, <slot 26> (CASE WHEN ((grouping(<slot 20> `GROUPING_PREFIX_`i_category``) + grouping(<slot 21> `GROUPING_PREFIX_`i_class``)) = 0) THEN <slot 17> `i_category` END) ASC, <slot 27> <slot 24> rank() OVER (PARTITION BY (grouping(`i_category`) + grouping(`i_class`)), (CASE WHEN (grouping(`i_class`) = 0) THEN `i_category` END) ORDER BY sum(`ws_net_paid`) DESC NULLS LAST) ASC") && 
		explainStr.contains("order by: <slot 51> <slot 22> sum(`ws_net_paid`) DESC NULLS LAST") && 
		explainStr.contains("order by: <slot 52> (grouping(<slot 20> `GROUPING_PREFIX_`i_category``) + grouping(<slot 21> `GROUPING_PREFIX_`i_class``)) ASC, <slot 53> (CASE WHEN (grouping(<slot 21> `GROUPING_PREFIX_`i_class``) = 0) THEN <slot 17> `i_category` END) ASC, <slot 51> <slot 22> sum(`ws_net_paid`) DESC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 22> sum(`ws_net_paid`))\n" + 
				"  |  group by: <slot 17> `i_category`, <slot 18> `i_class`, <slot 19> `GROUPING_ID`, <slot 20> `GROUPING_PREFIX_`i_category``, <slot 21> `GROUPING_PREFIX_`i_class``") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 13> `ws_net_paid`)\n" + 
				"  |  group by: <slot 11> `i_category`, <slot 12> `i_class`, <slot 14> `GROUPING_ID`, <slot 15> `GROUPING_PREFIX_`i_category``, <slot 16> `GROUPING_PREFIX_`i_class``") && 
		explainStr.contains("output slots: ``i_category``, ``i_class``, ``ws_net_paid``, ``GROUPING_ID``, ``GROUPING_PREFIX_`i_category```, ``GROUPING_PREFIX_`i_class```") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 33> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 36 37 38 39 40 41 42 43 \n" + 
				"  |  hash output slot ids: 32 33 1 34 2 35 9 31 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_sold_date_sk` = `d1`.`d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d1`.`d_date_sk`") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 31 32 33 34 35 \n" + 
				"  |  hash output slot ids: 0 6 7 8 10 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ws_sold_date_sk`") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d1`.`d_month_seq` >= 1200, `d1`.`d_month_seq` <= 1211") 
            
        }
    }
}