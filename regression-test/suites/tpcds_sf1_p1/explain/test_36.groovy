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

suite("test_regression_test_tpcds_sf1_p1_q36", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  (sum(ss_net_profit) / sum(ss_ext_sales_price)) gross_margin
		, i_category
		, i_class
		, (GROUPING (i_category) + GROUPING (i_class)) lochierarchy
		, rank() OVER (PARTITION BY (GROUPING (i_category) + GROUPING (i_class)), (CASE WHEN (GROUPING (i_class) = 0) THEN i_category END) ORDER BY (sum(ss_net_profit) / sum(ss_ext_sales_price)) ASC) rank_within_parent
		FROM
		  store_sales
		, date_dim d1
		, item
		, store
		WHERE (d1.d_year = 2001)
		   AND (d1.d_date_sk = ss_sold_date_sk)
		   AND (i_item_sk = ss_item_sk)
		   AND (s_store_sk = ss_store_sk)
		   AND (s_state IN (
		     'TN'
		   , 'TN'
		   , 'TN'
		   , 'TN'
		   , 'TN'
		   , 'TN'
		   , 'TN'
		   , 'TN'))
		GROUP BY ROLLUP (i_category, i_class)
		ORDER BY lochierarchy DESC, (CASE WHEN (lochierarchy = 0) THEN i_category END) ASC, rank_within_parent ASC, i_category, i_class
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 31> (grouping(<slot 25> `GROUPING_PREFIX_`i_category``) + grouping(<slot 26> `GROUPING_PREFIX_`i_class``)) DESC, <slot 32> (CASE WHEN ((grouping(<slot 25> `GROUPING_PREFIX_`i_category``) + grouping(<slot 26> `GROUPING_PREFIX_`i_class``)) = 0) THEN <slot 22> `i_category` END) ASC, <slot 33> <slot 30> rank() OVER (PARTITION BY (grouping(`i_category`) + grouping(`i_class`)), (CASE WHEN (grouping(`i_class`) = 0) THEN `i_category` END) ORDER BY (sum(`ss_net_profit`) / sum(`ss_ext_sales_price`)) ASC NULLS FIRST) ASC, <slot 34> <slot 22> `i_category` ASC, <slot 35> <slot 23> `i_class` ASC") && 
		explainStr.contains("order by: <slot 78> (<slot 27> sum(`ss_net_profit`) / <slot 28> sum(`ss_ext_sales_price`)) ASC NULLS FIRST") && 
		explainStr.contains("order by: <slot 76> (grouping(<slot 25> `GROUPING_PREFIX_`i_category``) + grouping(<slot 26> `GROUPING_PREFIX_`i_class``)) ASC, <slot 77> (CASE WHEN (grouping(<slot 26> `GROUPING_PREFIX_`i_class``) = 0) THEN <slot 22> `i_category` END) ASC, <slot 78> (<slot 27> sum(`ss_net_profit`) / <slot 28> sum(`ss_ext_sales_price`)) ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 27> sum(`ss_net_profit`)), sum(<slot 28> sum(`ss_ext_sales_price`))\n" + 
				"  |  group by: <slot 22> `i_category`, <slot 23> `i_class`, <slot 24> `GROUPING_ID`, <slot 25> `GROUPING_PREFIX_`i_category``, <slot 26> `GROUPING_PREFIX_`i_class``") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 17> `ss_net_profit`), sum(<slot 18> `ss_ext_sales_price`)\n" + 
				"  |  group by: <slot 15> `i_category`, <slot 16> `i_class`, <slot 19> `GROUPING_ID`, <slot 20> `GROUPING_PREFIX_`i_category``, <slot 21> `GROUPING_PREFIX_`i_class``") && 
		explainStr.contains("output slots: ``i_category``, ``i_class``, ``ss_net_profit``, ``ss_ext_sales_price``, ``GROUPING_ID``, ``GROUPING_PREFIX_`i_category```, ``GROUPING_PREFIX_`i_class```") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 49> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 55 56 57 58 59 60 61 62 63 64 65 66 \n" + 
				"  |  hash output slot ids: 48 49 50 51 52 53 54 12 45 46 14 47 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 41> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 45 46 47 48 49 50 51 52 53 54 \n" + 
				"  |  hash output slot ids: 2 3 38 39 40 41 42 10 43 44 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d1`.`d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d1`.`d_date_sk`") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 38 39 40 41 42 43 44 \n" + 
				"  |  hash output slot ids: 0 1 7 8 9 11 13 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`s_state` IN ('TN', 'TN', 'TN', 'TN', 'TN', 'TN', 'TN', 'TN'))") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d1`.`d_year` = 2001)") 
            
        }
    }
}