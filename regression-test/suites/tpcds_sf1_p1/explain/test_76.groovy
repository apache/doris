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

suite("test_regression_test_tpcds_sf1_p1_q76", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  channel
		, col_name
		, d_year
		, d_qoy
		, i_category
		, count(*) sales_cnt
		, sum(ext_sales_price) sales_amt
		FROM
		  (
		   SELECT
		     'store' channel
		   , 'ss_store_sk' col_name
		   , d_year
		   , d_qoy
		   , i_category
		   , ss_ext_sales_price ext_sales_price
		   FROM
		     store_sales
		   , item
		   , date_dim
		   WHERE (ss_store_sk IS NULL)
		      AND (ss_sold_date_sk = d_date_sk)
		      AND (ss_item_sk = i_item_sk)
		UNION ALL    SELECT
		     'web' channel
		   , 'ws_ship_customer_sk' col_name
		   , d_year
		   , d_qoy
		   , i_category
		   , ws_ext_sales_price ext_sales_price
		   FROM
		     web_sales
		   , item
		   , date_dim
		   WHERE (ws_ship_customer_sk IS NULL)
		      AND (ws_sold_date_sk = d_date_sk)
		      AND (ws_item_sk = i_item_sk)
		UNION ALL    SELECT
		     'catalog' channel
		   , 'cs_ship_addr_sk' col_name
		   , d_year
		   , d_qoy
		   , i_category
		   , cs_ext_sales_price ext_sales_price
		   FROM
		     catalog_sales
		   , item
		   , date_dim
		   WHERE (cs_ship_addr_sk IS NULL)
		      AND (cs_sold_date_sk = d_date_sk)
		      AND (cs_item_sk = i_item_sk)
		)  foo
		GROUP BY channel, col_name, d_year, d_qoy, i_category
		ORDER BY channel ASC, col_name ASC, d_year ASC, d_qoy ASC, i_category ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 46> <slot 39> `channel` ASC, <slot 47> <slot 40> `col_name` ASC, <slot 48> <slot 41> `d_year` ASC, <slot 49> <slot 42> `d_qoy` ASC, <slot 50> <slot 43> `i_category` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 44> count(*)), sum(<slot 45> sum(`ext_sales_price`))\n" + 
				"  |  group by: <slot 39> `channel`, <slot 40> `col_name`, <slot 41> `d_year`, <slot 42> `d_qoy`, <slot 43> `i_category`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: count(*), sum(<slot 32> `ss_ext_sales_price` `ws_ext_sales_price` `cs_ext_sales_price`)\n" + 
				"  |  group by: <slot 27> 'store' 'web' 'catalog', <slot 28> 'ss_store_sk' 'ws_ship_customer_sk' 'cs_ship_addr_sk', <slot 29> `d_year` `d_year` `d_year`, <slot 30> `d_qoy` `d_qoy` `d_qoy`, <slot 31> `i_category` `i_category` `i_category`") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 88> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 18") && 
		explainStr.contains("output slot ids: 92 93 94 95 96 97 98 99 100 \n" + 
				"  |  hash output slot ids: 20 85 86 87 88 89 90 26 91 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 17") && 
		explainStr.contains("output slot ids: 85 86 87 88 89 90 91 \n" + 
				"  |  hash output slot ids: 18 19 21 22 23 24 25 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`cs_ship_addr_sk` IS NULL)\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `cs_sold_date_sk`") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 72> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 76 77 78 79 80 81 82 83 84 \n" + 
				"  |  hash output slot ids: 17 69 70 71 72 73 74 75 11 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 69 70 71 72 73 74 75 \n" + 
				"  |  hash output slot ids: 16 9 10 12 13 14 15 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ws_ship_customer_sk` IS NULL)\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `ws_sold_date_sk`") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 56> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 60 61 62 63 64 65 66 67 68 \n" + 
				"  |  hash output slot ids: 2 53 54 55 56 8 57 58 59 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 53 54 55 56 57 58 59 \n" + 
				"  |  hash output slot ids: 0 1 3 4 5 6 7 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ss_store_sk` IS NULL)\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") 
            
        }
    }
}