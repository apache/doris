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

suite("test_regression_test_tpcds_sf1_p1_q80", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  ssr AS (
		   SELECT
		     s_store_id store_id
		   , sum(ss_ext_sales_price) sales
		   , sum(COALESCE(sr_return_amt, 0)) returns
		   , sum((ss_net_profit - COALESCE(sr_net_loss, 0))) profit
		   FROM
		     store_sales
		   LEFT JOIN store_returns ON (ss_item_sk = sr_item_sk)
		      AND (ss_ticket_number = sr_ticket_number)
		   , date_dim
		   , store
		   , item
		   , promotion
		   WHERE (ss_sold_date_sk = d_date_sk)
		      AND (CAST(d_date AS DATE) BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
		      AND (ss_store_sk = s_store_sk)
		      AND (ss_item_sk = i_item_sk)
		      AND (i_current_price > 50)
		      AND (ss_promo_sk = p_promo_sk)
		      AND (p_channel_tv = 'N')
		   GROUP BY s_store_id
		)
		, csr AS (
		   SELECT
		     cp_catalog_page_id catalog_page_id
		   , sum(cs_ext_sales_price) sales
		   , sum(COALESCE(cr_return_amount, 0)) returns
		   , sum((cs_net_profit - COALESCE(cr_net_loss, 0))) profit
		   FROM
		     catalog_sales
		   LEFT JOIN catalog_returns ON (cs_item_sk = cr_item_sk)
		      AND (cs_order_number = cr_order_number)
		   , date_dim
		   , catalog_page
		   , item
		   , promotion
		   WHERE (cs_sold_date_sk = d_date_sk)
		      AND (CAST(d_date AS DATE) BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
		      AND (cs_catalog_page_sk = cp_catalog_page_sk)
		      AND (cs_item_sk = i_item_sk)
		      AND (i_current_price > 50)
		      AND (cs_promo_sk = p_promo_sk)
		      AND (p_channel_tv = 'N')
		   GROUP BY cp_catalog_page_id
		)
		, wsr AS (
		   SELECT
		     web_site_id
		   , sum(ws_ext_sales_price) sales
		   , sum(COALESCE(wr_return_amt, 0)) returns
		   , sum((ws_net_profit - COALESCE(wr_net_loss, 0))) profit
		   FROM
		     web_sales
		   LEFT JOIN web_returns ON (ws_item_sk = wr_item_sk)
		      AND (ws_order_number = wr_order_number)
		   , date_dim
		   , web_site
		   , item
		   , promotion
		   WHERE (ws_sold_date_sk = d_date_sk)
		      AND (CAST(d_date AS DATE) BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
		      AND (ws_web_site_sk = web_site_sk)
		      AND (ws_item_sk = i_item_sk)
		      AND (i_current_price > 50)
		      AND (ws_promo_sk = p_promo_sk)
		      AND (p_channel_tv = 'N')
		   GROUP BY web_site_id
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
		   , concat('store', store_id) id
		   , sales
		   , returns
		   , profit
		   FROM
		     ssr
		UNION ALL    SELECT
		     'catalog channel' channel
		   , concat('catalog_page', catalog_page_id) id
		   , sales
		   , returns
		   , profit
		   FROM
		     csr
		UNION ALL    SELECT
		     'web channel' channel
		   , concat('web_site', web_site_id) id
		   , sales
		   , returns
		   , profit
		   FROM
		     wsr
		)  x
		GROUP BY ROLLUP (channel, id)
		ORDER BY channel ASC, id ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 104> <slot 98> `channel` ASC, <slot 105> <slot 99> `id` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 101> sum(`sales`)), sum(<slot 102> sum(`returns`)), sum(<slot 103> sum(`profit`))\n" + 
				"  |  group by: <slot 98> `channel`, <slot 99> `id`, <slot 100> `GROUPING_ID`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 94> `sales`), sum(<slot 95> `returns`), sum(<slot 96> `profit`)\n" + 
				"  |  group by: <slot 92> `channel`, <slot 93> `id`, <slot 97> `GROUPING_ID`") && 
		explainStr.contains("output slots: ``channel``, ``id``, ``sales``, ``returns``, ``profit``, ``GROUPING_ID``") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 74> sum(`ws_ext_sales_price`)), sum(<slot 75> sum(coalesce(`wr_return_amt`, 0))), sum(<slot 76> sum((`ws_net_profit` - coalesce(`wr_net_loss`, 0))))\n" + 
				"  |  group by: <slot 73> `web_site_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 293>), sum(coalesce(<slot 308>, 0)), sum((<slot 294> - coalesce(<slot 309>, 0)))\n" + 
				"  |  group by: <slot 300>") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 276> = `wr_item_sk`)\n" + 
				"  |  equal join conjunct: (<slot 277> = `wr_order_number`)") && 
		explainStr.contains("vec output tuple id: 44") && 
		explainStr.contains("output slot ids: 293 294 300 308 309 \n" + 
				"  |  hash output slot ids: 278 279 60 285 62 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 269> = `p_promo_sk`)") && 
		explainStr.contains("vec output tuple id: 43") && 
		explainStr.contains("output slot ids: 276 277 278 279 285 \n" + 
				"  |  hash output slot ids: 272 263 264 265 266 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 252> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 42") && 
		explainStr.contains("output slot ids: 263 264 265 266 269 272 \n" + 
				"  |  hash output slot ids: 258 261 252 253 254 255 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 248> = `web_site_sk`)") && 
		explainStr.contains("vec output tuple id: 41") && 
		explainStr.contains("output slot ids: 252 253 254 255 258 261 \n" + 
				"  |  hash output slot ids: 243 244 245 246 249 58 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 40") && 
		explainStr.contains("output slot ids: 243 244 245 246 248 249 \n" + 
				"  |  hash output slot ids: 66 54 70 56 59 61 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `ws_sold_date_sk`") && 
		explainStr.contains("TABLE: web_returns(web_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: promotion(promotion), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`p_channel_tv` = 'N')") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_current_price` > 50)") && 
		explainStr.contains("TABLE: web_site(web_site), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: CAST(`d_date` AS DATE) >= '2000-08-23 00:00:00', CAST(`d_date` AS DATE) <= '2000-09-22 00:00:00'") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 47> sum(`cs_ext_sales_price`)), sum(<slot 48> sum(coalesce(`cr_return_amount`, 0))), sum(<slot 49> sum((`cs_net_profit` - coalesce(`cr_net_loss`, 0))))\n" + 
				"  |  group by: <slot 46> `cp_catalog_page_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 226>), sum(coalesce(<slot 241>, 0)), sum((<slot 227> - coalesce(<slot 242>, 0)))\n" + 
				"  |  group by: <slot 233>") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 209> = `cr_item_sk`)\n" + 
				"  |  equal join conjunct: (<slot 210> = `cr_order_number`)") && 
		explainStr.contains("vec output tuple id: 39") && 
		explainStr.contains("output slot ids: 226 227 233 241 242 \n" + 
				"  |  hash output slot ids: 33 211 35 212 218 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 202> = `p_promo_sk`)") && 
		explainStr.contains("vec output tuple id: 38") && 
		explainStr.contains("output slot ids: 209 210 211 212 218 \n" + 
				"  |  hash output slot ids: 196 197 198 199 205 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 185> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 37") && 
		explainStr.contains("output slot ids: 196 197 198 199 202 205 \n" + 
				"  |  hash output slot ids: 194 185 186 187 188 191 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 181> = `cp_catalog_page_sk`)") && 
		explainStr.contains("vec output tuple id: 36") && 
		explainStr.contains("output slot ids: 185 186 187 188 191 194 \n" + 
				"  |  hash output slot ids: 176 177 178 179 182 31 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 35") && 
		explainStr.contains("output slot ids: 176 177 178 179 181 182 \n" + 
				"  |  hash output slot ids: 32 34 39 27 43 29 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `cs_sold_date_sk`") && 
		explainStr.contains("TABLE: catalog_returns(catalog_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: promotion(promotion), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`p_channel_tv` = 'N')") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_current_price` > 50)") && 
		explainStr.contains("TABLE: catalog_page(catalog_page), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: CAST(`d_date` AS DATE) >= '2000-08-23 00:00:00', CAST(`d_date` AS DATE) <= '2000-09-22 00:00:00'") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 20> sum(`ss_ext_sales_price`)), sum(<slot 21> sum(coalesce(`sr_return_amt`, 0))), sum(<slot 22> sum((`ss_net_profit` - coalesce(`sr_net_loss`, 0))))\n" + 
				"  |  group by: <slot 19> `s_store_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 159>), sum(coalesce(<slot 174>, 0)), sum((<slot 160> - coalesce(<slot 175>, 0)))\n" + 
				"  |  group by: <slot 166>") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 142> = `sr_item_sk`)\n" + 
				"  |  equal join conjunct: (<slot 143> = `sr_ticket_number`)") && 
		explainStr.contains("vec output tuple id: 34") && 
		explainStr.contains("output slot ids: 159 160 166 174 175 \n" + 
				"  |  hash output slot ids: 144 145 6 151 8 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 135> = `p_promo_sk`)") && 
		explainStr.contains("vec output tuple id: 33") && 
		explainStr.contains("output slot ids: 142 143 144 145 151 \n" + 
				"  |  hash output slot ids: 129 130 131 132 138 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 118> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 32") && 
		explainStr.contains("output slot ids: 129 130 131 132 135 138 \n" + 
				"  |  hash output slot ids: 118 119 120 121 124 127 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 114> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 31") && 
		explainStr.contains("output slot ids: 118 119 120 121 124 127 \n" + 
				"  |  hash output slot ids: 112 115 4 109 110 111 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 30") && 
		explainStr.contains("output slot ids: 109 110 111 112 114 115 \n" + 
				"  |  hash output slot ids: 0 16 2 5 7 12 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: store_returns(store_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: promotion(promotion), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`p_channel_tv` = 'N')") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_current_price` > 50)") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: CAST(`d_date` AS DATE) >= '2000-08-23 00:00:00', CAST(`d_date` AS DATE) <= '2000-09-22 00:00:00'") 
            
        }
    }
}