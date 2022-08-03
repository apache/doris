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

suite("test_regression_test_tpcds_sf1_p1_q77", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  ss AS (
		   SELECT
		     s_store_sk
		   , sum(ss_ext_sales_price) sales
		   , sum(ss_net_profit) profit
		   FROM
		     store_sales
		   , date_dim
		   , store
		   WHERE (ss_sold_date_sk = d_date_sk)
		      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
		      AND (ss_store_sk = s_store_sk)
		   GROUP BY s_store_sk
		)
		, sr AS (
		   SELECT
		     s_store_sk
		   , sum(sr_return_amt) returns
		   , sum(sr_net_loss) profit_loss
		   FROM
		     store_returns
		   , date_dim
		   , store
		   WHERE (sr_returned_date_sk = d_date_sk)
		      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
		      AND (sr_store_sk = s_store_sk)
		   GROUP BY s_store_sk
		)
		, cs AS (
		   SELECT
		     cs_call_center_sk
		   , sum(cs_ext_sales_price) sales
		   , sum(cs_net_profit) profit
		   FROM
		     catalog_sales
		   , date_dim
		   WHERE (cs_sold_date_sk = d_date_sk)
		      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
		   GROUP BY cs_call_center_sk
		)
		, cr AS (
		   SELECT
		     cr_call_center_sk
		   , sum(cr_return_amount) returns
		   , sum(cr_net_loss) profit_loss
		   FROM
		     catalog_returns
		   , date_dim
		   WHERE (cr_returned_date_sk = d_date_sk)
		      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
		   GROUP BY cr_call_center_sk
		)
		, ws AS (
		   SELECT
		     wp_web_page_sk
		   , sum(ws_ext_sales_price) sales
		   , sum(ws_net_profit) profit
		   FROM
		     web_sales
		   , date_dim
		   , web_page
		   WHERE (ws_sold_date_sk = d_date_sk)
		      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
		      AND (ws_web_page_sk = wp_web_page_sk)
		   GROUP BY wp_web_page_sk
		)
		, wr AS (
		   SELECT
		     wp_web_page_sk
		   , sum(wr_return_amt) returns
		   , sum(wr_net_loss) profit_loss
		   FROM
		     web_returns
		   , date_dim
		   , web_page
		   WHERE (wr_returned_date_sk = d_date_sk)
		      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
		      AND (wr_web_page_sk = wp_web_page_sk)
		   GROUP BY wp_web_page_sk
		)
		SELECT
		  channel
		, id
		, sum(sales) sales
		, sum(returns) returns
		, sum(profit) profit
		FROM
		  (
		   SELECT
		     'store channel' channel
		   , ss.s_store_sk id
		   , sales
		   , COALESCE(returns, 0) returns
		   , (profit - COALESCE(profit_loss, 0)) profit
		   FROM
		     ss
		   LEFT JOIN sr ON (ss.s_store_sk = sr.s_store_sk)
		UNION ALL    SELECT
		     'catalog channel' channel
		   , cs_call_center_sk id
		   , sales
		   , returns
		   , (profit - profit_loss) profit
		   FROM
		     cs
		   , cr
		UNION ALL    SELECT
		     'web channel' channel
		   , ws.wp_web_page_sk id
		   , sales
		   , COALESCE(returns, 0) returns
		   , (profit - COALESCE(profit_loss, 0)) profit
		   FROM
		     ws
		   LEFT JOIN wr ON (ws.wp_web_page_sk = wr.wp_web_page_sk)
		)  x
		GROUP BY ROLLUP (channel, id)
		ORDER BY channel ASC, id ASC, sales ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 99> <slot 93> `channel` ASC, <slot 100> <slot 94> `id` ASC, <slot 101> <slot 96> sum(`sales`) ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 96> sum(`sales`)), sum(<slot 97> sum(`returns`)), sum(<slot 98> sum(`profit`))\n" + 
				"  |  group by: <slot 93> `channel`, <slot 94> `id`, <slot 95> `GROUPING_ID`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 89> `sales`), sum(<slot 90> `returns`), sum(<slot 91> `profit`)\n" + 
				"  |  group by: <slot 87> `channel`, <slot 88> `id`, <slot 92> `GROUPING_ID`") && 
		explainStr.contains("output slots: ``channel``, ``id``, ``sales``, ``returns``, ``profit``, ``GROUPING_ID``") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 57> `wp_web_page_sk` = <slot 70> `wp_web_page_sk`)") && 
		explainStr.contains("vec output tuple id: 45") && 
		explainStr.contains("output slot ids: 174 175 176 177 178 179 \n" + 
				"  |  hash output slot ids: 70 71 72 57 58 59 ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 58> sum(`ws_ext_sales_price`)), sum(<slot 59> sum(`ws_net_profit`))\n" + 
				"  |  group by: <slot 57> `wp_web_page_sk`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 71> sum(`wr_return_amt`)), sum(<slot 72> sum(`wr_net_loss`))\n" + 
				"  |  group by: <slot 70> `wp_web_page_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 169>), sum(<slot 170>)\n" + 
				"  |  group by: <slot 173>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 166> = `wp_web_page_sk`)") && 
		explainStr.contains("vec output tuple id: 44") && 
		explainStr.contains("output slot ids: 169 170 173 \n" + 
				"  |  hash output slot ids: 163 164 63 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`d_date_sk` = `wr_returned_date_sk`)\n" + 
				"  |  runtime filters: RF005[in_or_bloom] <- `wr_returned_date_sk`") && 
		explainStr.contains("vec output tuple id: 43") && 
		explainStr.contains("output slot ids: 163 164 166 \n" + 
				"  |  hash output slot ids: 64 65 69 ") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_date` >= '2000-08-23 00:00:00', `d_date` <= '2000-09-22 00:00:00'\n" + 
				"     runtime filters: RF005[in_or_bloom] -> `d_date_sk`") && 
		explainStr.contains("TABLE: web_page(web_page), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: web_returns(web_returns), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 154>), sum(<slot 155>)\n" + 
				"  |  group by: <slot 160>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 151> = `wp_web_page_sk`)") && 
		explainStr.contains("vec output tuple id: 42") && 
		explainStr.contains("output slot ids: 154 155 160 \n" + 
				"  |  hash output slot ids: 50 148 149 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF004[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 41") && 
		explainStr.contains("output slot ids: 148 149 151 \n" + 
				"  |  hash output slot ids: 51 52 56 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF004[in_or_bloom] -> `ws_sold_date_sk`") && 
		explainStr.contains("TABLE: web_page(web_page), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_date` >= '2000-08-23 00:00:00', `d_date` <= '2000-09-22 00:00:00'") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates is NULL.  |  cardinality=-1") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 33> sum(`cs_ext_sales_price`)), sum(<slot 34> sum(`cs_net_profit`))\n" + 
				"  |  group by: <slot 32> `cs_call_center_sk`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 45> sum(`cr_return_amount`)), sum(<slot 46> sum(`cr_net_loss`))\n" + 
				"  |  group by: <slot 44> `cr_call_center_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 143>), sum(<slot 144>)\n" + 
				"  |  group by: <slot 142>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cr_returned_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 40") && 
		explainStr.contains("output slot ids: 142 143 144 \n" + 
				"  |  hash output slot ids: 38 39 40 ") && 
		explainStr.contains("TABLE: catalog_returns(catalog_returns), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF003[in_or_bloom] -> `cr_returned_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_date` >= '2000-08-23 00:00:00', `d_date` <= '2000-09-22 00:00:00'") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 137>), sum(<slot 138>)\n" + 
				"  |  group by: <slot 136>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 39") && 
		explainStr.contains("output slot ids: 136 137 138 \n" + 
				"  |  hash output slot ids: 26 27 28 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `cs_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_date` >= '2000-08-23 00:00:00', `d_date` <= '2000-09-22 00:00:00'") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 7> `s_store_sk` = <slot 20> `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 38") && 
		explainStr.contains("output slot ids: 130 131 132 133 134 135 \n" + 
				"  |  hash output slot ids: 20 21 22 7 8 9 ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 8> sum(`ss_ext_sales_price`)), sum(<slot 9> sum(`ss_net_profit`))\n" + 
				"  |  group by: <slot 7> `s_store_sk`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 21> sum(`sr_return_amt`)), sum(<slot 22> sum(`sr_net_loss`))\n" + 
				"  |  group by: <slot 20> `s_store_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 123>), sum(<slot 124>)\n" + 
				"  |  group by: <slot 129>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 120> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 37") && 
		explainStr.contains("output slot ids: 123 124 129 \n" + 
				"  |  hash output slot ids: 117 118 13 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`sr_returned_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 36") && 
		explainStr.contains("output slot ids: 117 118 120 \n" + 
				"  |  hash output slot ids: 19 14 15 ") && 
		explainStr.contains("TABLE: store_returns(store_returns), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `sr_returned_date_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_date` >= '2000-08-23 00:00:00', `d_date` <= '2000-09-22 00:00:00'") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 110>), sum(<slot 111>)\n" + 
				"  |  group by: <slot 116>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 107> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 35") && 
		explainStr.contains("output slot ids: 110 111 116 \n" + 
				"  |  hash output slot ids: 0 104 105 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 34") && 
		explainStr.contains("output slot ids: 104 105 107 \n" + 
				"  |  hash output slot ids: 1 2 6 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_date` >= '2000-08-23 00:00:00', `d_date` <= '2000-09-22 00:00:00'") 
            
        }
    }
}