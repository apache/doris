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

suite("test_regression_test_tpcds_sf1_p1_q05", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  ssr AS (
		   SELECT
		     s_store_id
		   , sum(sales_price) sales
		   , sum(profit) profit
		   , sum(return_amt) returns
		   , sum(net_loss) profit_loss
		   FROM
		     (
		      SELECT
		        ss_store_sk store_sk
		      , ss_sold_date_sk date_sk
		      , ss_ext_sales_price sales_price
		      , ss_net_profit profit
		      , CAST(0 AS DECIMAL(7,2)) return_amt
		      , CAST(0 AS DECIMAL(7,2)) net_loss
		      FROM
		        store_sales
		UNION ALL       SELECT
		        sr_store_sk store_sk
		      , sr_returned_date_sk date_sk
		      , CAST(0 AS DECIMAL(7,2)) sales_price
		      , CAST(0 AS DECIMAL(7,2)) profit
		      , sr_return_amt return_amt
		      , sr_net_loss net_loss
		      FROM
		        store_returns
		   )  salesreturns
		   , date_dim
		   , store
		   WHERE (date_sk = d_date_sk)
		      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '14' DAY))
		      AND (store_sk = s_store_sk)
		   GROUP BY s_store_id
		)
		, csr AS (
		   SELECT
		     cp_catalog_page_id
		   , sum(sales_price) sales
		   , sum(profit) profit
		   , sum(return_amt) returns
		   , sum(net_loss) profit_loss
		   FROM
		     (
		      SELECT
		        cs_catalog_page_sk page_sk
		      , cs_sold_date_sk date_sk
		      , cs_ext_sales_price sales_price
		      , cs_net_profit profit
		      , CAST(0 AS DECIMAL(7,2)) return_amt
		      , CAST(0 AS DECIMAL(7,2)) net_loss
		      FROM
		        catalog_sales
		UNION ALL       SELECT
		        cr_catalog_page_sk page_sk
		      , cr_returned_date_sk date_sk
		      , CAST(0 AS DECIMAL(7,2)) sales_price
		      , CAST(0 AS DECIMAL(7,2)) profit
		      , cr_return_amount return_amt
		      , cr_net_loss net_loss
		      FROM
		        catalog_returns
		   )  salesreturns
		   , date_dim
		   , catalog_page
		   WHERE (date_sk = d_date_sk)
		      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '14' DAY))
		      AND (page_sk = cp_catalog_page_sk)
		   GROUP BY cp_catalog_page_id
		)
		, wsr AS (
		   SELECT
		     web_site_id
		   , sum(sales_price) sales
		   , sum(profit) profit
		   , sum(return_amt) returns
		   , sum(net_loss) profit_loss
		   FROM
		     (
		      SELECT
		        ws_web_site_sk wsr_web_site_sk
		      , ws_sold_date_sk date_sk
		      , ws_ext_sales_price sales_price
		      , ws_net_profit profit
		      , CAST(0 AS DECIMAL(7,2)) return_amt
		      , CAST(0 AS DECIMAL(7,2)) net_loss
		      FROM
		        web_sales
		UNION ALL       SELECT
		        ws_web_site_sk wsr_web_site_sk
		      , wr_returned_date_sk date_sk
		      , CAST(0 AS DECIMAL(7,2)) sales_price
		      , CAST(0 AS DECIMAL(7,2)) profit
		      , wr_return_amt return_amt
		      , wr_net_loss net_loss
		      FROM
		        web_returns
		      LEFT JOIN web_sales ON (wr_item_sk = ws_item_sk)
		         AND (wr_order_number = ws_order_number)
		   )  salesreturns
		   , date_dim
		   , web_site
		   WHERE (date_sk = d_date_sk)
		      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '14' DAY))
		      AND (wsr_web_site_sk = web_site_sk)
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
		   , concat('store', s_store_id) id
		   , sales
		   , returns
		   , (profit - profit_loss) profit
		   FROM
		     ssr
		UNION ALL    SELECT
		     'catalog channel' channel
		   , concat('catalog_page', cp_catalog_page_id) id
		   , sales
		   , returns
		   , (profit - profit_loss) profit
		   FROM
		     csr
		UNION ALL    SELECT
		     'web channel' channel
		   , concat('web_site', web_site_id) id
		   , sales
		   , returns
		   , (profit - profit_loss) profit
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
				"  |  order by: <slot 129> <slot 123> `channel` ASC, <slot 130> <slot 124> `id` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 126> sum(`sales`)), sum(<slot 127> sum(`returns`)), sum(<slot 128> sum(`profit`))\n" + 
				"  |  group by: <slot 123> `channel`, <slot 124> `id`, <slot 125> `GROUPING_ID`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 119> `sales`), sum(<slot 120> `returns`), sum(<slot 121> `profit`)\n" + 
				"  |  group by: <slot 117> `channel`, <slot 118> `id`, <slot 122> `GROUPING_ID`") && 
		explainStr.contains("output slots: ``channel``, ``id``, ``sales``, ``returns``, ``profit``, ``GROUPING_ID``") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 97> sum(`sales_price`)), sum(<slot 98> sum(`profit`)), sum(<slot 99> sum(`return_amt`)), sum(<slot 100> sum(`net_loss`))\n" + 
				"  |  group by: <slot 96> `web_site_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 188>), sum(<slot 189>), sum(<slot 190>), sum(<slot 191>)\n" + 
				"  |  group by: <slot 194>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 178> = `web_site_sk`)") && 
		explainStr.contains("vec output tuple id: 37") && 
		explainStr.contains("output slot ids: 188 189 190 191 194 \n" + 
				"  |  hash output slot ids: 180 181 182 183 92 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Src column hash been transformed by expr]\n" + 
				"  |  equal join conjunct: (<slot 81> `ws_sold_date_sk` `wr_returned_date_sk` = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 36") && 
		explainStr.contains("output slot ids: 178 180 181 182 183 \n" + 
				"  |  hash output slot ids: 80 82 83 84 85 ") && 
		explainStr.contains("TABLE: web_site(web_site), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_date` >= '2000-08-23 00:00:00', `d_date` <= '2000-09-06 00:00:00'") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`wr_item_sk` = `ws_item_sk`)\n" + 
				"  |  equal join conjunct: (`wr_order_number` = `ws_order_number`)") && 
		explainStr.contains("vec output tuple id: 35") && 
		explainStr.contains("output slot ids: 170 171 172 173 174 175 176 177 \n" + 
				"  |  hash output slot ids: 72 73 74 75 76 77 78 79 ") && 
		explainStr.contains("TABLE: web_returns(web_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 59> sum(`sales_price`)), sum(<slot 60> sum(`profit`)), sum(<slot 61> sum(`return_amt`)), sum(<slot 62> sum(`net_loss`))\n" + 
				"  |  group by: <slot 58> `cp_catalog_page_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 162>), sum(<slot 163>), sum(<slot 164>), sum(<slot 165>)\n" + 
				"  |  group by: <slot 168>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 152> = `cp_catalog_page_sk`)") && 
		explainStr.contains("vec output tuple id: 34") && 
		explainStr.contains("output slot ids: 162 163 164 165 168 \n" + 
				"  |  hash output slot ids: 54 154 155 156 157 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Src column hash been transformed by expr]\n" + 
				"  |  equal join conjunct: (<slot 43> `cs_sold_date_sk` `cr_returned_date_sk` = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 33") && 
		explainStr.contains("output slot ids: 152 154 155 156 157 \n" + 
				"  |  hash output slot ids: 42 44 45 46 47 ") && 
		explainStr.contains("TABLE: catalog_page(catalog_page), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_date` >= '2000-08-23 00:00:00', `d_date` <= '2000-09-06 00:00:00'") && 
		explainStr.contains("TABLE: catalog_returns(catalog_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 25> sum(`sales_price`)), sum(<slot 26> sum(`profit`)), sum(<slot 27> sum(`return_amt`)), sum(<slot 28> sum(`net_loss`))\n" + 
				"  |  group by: <slot 24> `s_store_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 144>), sum(<slot 145>), sum(<slot 146>), sum(<slot 147>)\n" + 
				"  |  group by: <slot 150>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 134> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 32") && 
		explainStr.contains("output slot ids: 144 145 146 147 150 \n" + 
				"  |  hash output slot ids: 20 136 137 138 139 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Src column hash been transformed by expr]\n" + 
				"  |  equal join conjunct: (<slot 9> `ss_sold_date_sk` `sr_returned_date_sk` = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 31") && 
		explainStr.contains("output slot ids: 134 136 137 138 139 \n" + 
				"  |  hash output slot ids: 8 10 11 12 13 ") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_date` >= '2000-08-23 00:00:00', `d_date` <= '2000-09-06 00:00:00'") && 
		explainStr.contains("TABLE: store_returns(store_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON") 
            
        }
    }
}