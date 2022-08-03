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

suite("test_regression_test_tpcds_sf1_p1_q49", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		(SELECT
		  'web' channel
		, web.item
		, web.return_ratio
		, web.return_rank
		, web.currency_rank
		FROM
		  (
		   SELECT
		     item
		   , return_ratio
		   , currency_ratio
		   , rank() OVER (ORDER BY return_ratio ASC) return_rank
		   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
		   FROM
		     (
		      SELECT
		        ws.ws_item_sk item
		      , (CAST(sum(COALESCE(wr.wr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(ws.ws_quantity, 0)) AS DECIMAL(15,4))) return_ratio
		      , (CAST(sum(COALESCE(wr.wr_return_amt, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(ws.ws_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
		      FROM
		        web_sales ws
		      LEFT JOIN web_returns wr ON (ws.ws_order_number = wr.wr_order_number)
		         AND (ws.ws_item_sk = wr.wr_item_sk)
		      , date_dim
		      WHERE (wr.wr_return_amt > 10000)
		         AND (ws.ws_net_profit > 1)
		         AND (ws.ws_net_paid > 0)
		         AND (ws.ws_quantity > 0)
		         AND (ws_sold_date_sk = d_date_sk)
		         AND (d_year = 2001)
		         AND (d_moy = 12)
		      GROUP BY ws.ws_item_sk
		   )  in_web
		)  web
		WHERE (web.return_rank <= 10)
		   OR (web.currency_rank <= 10))
		UNION (SELECT
		  'catalog' channel
		, catalog.item
		, catalog.return_ratio
		, catalog.return_rank
		, catalog.currency_rank
		FROM
		  (
		   SELECT
		     item
		   , return_ratio
		   , currency_ratio
		   , rank() OVER (ORDER BY return_ratio ASC) return_rank
		   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
		   FROM
		     (
		      SELECT
		        cs.cs_item_sk item
		      , (CAST(sum(COALESCE(cr.cr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(cs.cs_quantity, 0)) AS DECIMAL(15,4))) return_ratio
		      , (CAST(sum(COALESCE(cr.cr_return_amount, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(cs.cs_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
		      FROM
		        catalog_sales cs
		      LEFT JOIN catalog_returns cr ON (cs.cs_order_number = cr.cr_order_number)
		         AND (cs.cs_item_sk = cr.cr_item_sk)
		      , date_dim
		      WHERE (cr.cr_return_amount > 10000)
		         AND (cs.cs_net_profit > 1)
		         AND (cs.cs_net_paid > 0)
		         AND (cs.cs_quantity > 0)
		         AND (cs_sold_date_sk = d_date_sk)
		         AND (d_year = 2001)
		         AND (d_moy = 12)
		      GROUP BY cs.cs_item_sk
		   )  in_cat
		) catalog 
		WHERE (catalog.return_rank <= 10)
		   OR (catalog.currency_rank <= 10))
		UNION (SELECT
		  'store' channel
		, store.item
		, store.return_ratio
		, store.return_rank
		, store.currency_rank
		FROM
		  (
		   SELECT
		     item
		   , return_ratio
		   , currency_ratio
		   , rank() OVER (ORDER BY return_ratio ASC) return_rank
		   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
		   FROM
		     (
		      SELECT
		        sts.ss_item_sk item
		      , (CAST(sum(COALESCE(sr.sr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(sts.ss_quantity, 0)) AS DECIMAL(15,4))) return_ratio
		      , (CAST(sum(COALESCE(sr.sr_return_amt, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(sts.ss_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
		      FROM
		        store_sales sts
		      LEFT JOIN store_returns sr ON (sts.ss_ticket_number = sr.sr_ticket_number)
		         AND (sts.ss_item_sk = sr.sr_item_sk)
		      , date_dim
		      WHERE (sr.sr_return_amt > 10000)
		         AND (sts.ss_net_profit > 1)
		         AND (sts.ss_net_paid > 0)
		         AND (sts.ss_quantity > 0)
		         AND (ss_sold_date_sk = d_date_sk)
		         AND (d_year = 2001)
		         AND (d_moy = 12)
		      GROUP BY sts.ss_item_sk
		   )  in_store
		)  store
		WHERE (store.return_rank <= 10)
		   OR (store.currency_rank <= 10))
		ORDER BY 1 ASC, 4 ASC, 5 ASC, 2 ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 95> <slot 90> 'web' 'catalog' 'store' ASC, <slot 96> <slot 93> `web`.`return_rank` `catalog`.`return_rank` `store`.`return_rank` ASC, <slot 97> <slot 94> `web`.`currency_rank` `catalog`.`currency_rank` `store`.`currency_rank` ASC, <slot 98> <slot 91> `web`.`item` `catalog`.`item` `store`.`item` ASC") && 
		explainStr.contains("VAGGREGATE (update finalize)\n" + 
				"  |  group by: <slot 90> 'web' 'catalog' 'store', <slot 91> `web`.`item` `catalog`.`item` `store`.`item`, <slot 92> `web`.`return_ratio` `catalog`.`return_ratio` `store`.`return_ratio`, <slot 93> `web`.`return_rank` `catalog`.`return_rank` `store`.`return_rank`, <slot 94> `web`.`currency_rank` `catalog`.`currency_rank` `store`.`currency_rank`") && 
		explainStr.contains("predicates: ((<slot 198> <slot 176> <= 10) OR (<slot 178> <= 10))") && 
		explainStr.contains("order by: (CAST(<slot 195> <slot 183> <slot 46> sum(coalesce(`cr`.`cr_return_amount`, 0)) AS DECIMAL(15,4)) / CAST(<slot 196> <slot 184> <slot 47> sum(coalesce(`cs`.`cs_net_paid`, 0)) AS DECIMAL(15,4))) ASC NULLS FIRST") && 
		explainStr.contains("order by: <slot 199> `currency_ratio` ASC") && 
		explainStr.contains("order by: (CAST(<slot 181> <slot 44> sum(coalesce(`cr`.`cr_return_quantity`, 0)) AS DECIMAL(15,4)) / CAST(<slot 182> <slot 45> sum(coalesce(`cs`.`cs_quantity`, 0)) AS DECIMAL(15,4))) ASC NULLS FIRST") && 
		explainStr.contains("predicates: ((<slot 252> <slot 230> <= 10) OR (<slot 232> <= 10))") && 
		explainStr.contains("order by: (CAST(<slot 249> <slot 237> <slot 76> sum(coalesce(`sr`.`sr_return_amt`, 0)) AS DECIMAL(15,4)) / CAST(<slot 250> <slot 238> <slot 77> sum(coalesce(`sts`.`ss_net_paid`, 0)) AS DECIMAL(15,4))) ASC NULLS FIRST") && 
		explainStr.contains("order by: <slot 253> `currency_ratio` ASC") && 
		explainStr.contains("order by: (CAST(<slot 235> <slot 74> sum(coalesce(`sr`.`sr_return_quantity`, 0)) AS DECIMAL(15,4)) / CAST(<slot 236> <slot 75> sum(coalesce(`sts`.`ss_quantity`, 0)) AS DECIMAL(15,4))) ASC NULLS FIRST") && 
		explainStr.contains("predicates: ((<slot 144> <slot 122> <= 10) OR (<slot 124> <= 10))") && 
		explainStr.contains("order by: (CAST(<slot 141> <slot 129> <slot 16> sum(coalesce(`wr`.`wr_return_amt`, 0)) AS DECIMAL(15,4)) / CAST(<slot 142> <slot 130> <slot 17> sum(coalesce(`ws`.`ws_net_paid`, 0)) AS DECIMAL(15,4))) ASC NULLS FIRST") && 
		explainStr.contains("order by: <slot 145> `currency_ratio` ASC") && 
		explainStr.contains("order by: (CAST(<slot 127> <slot 14> sum(coalesce(`wr`.`wr_return_quantity`, 0)) AS DECIMAL(15,4)) / CAST(<slot 128> <slot 15> sum(coalesce(`ws`.`ws_quantity`, 0)) AS DECIMAL(15,4))) ASC NULLS FIRST") && 
		explainStr.contains("order by: <slot 239> `return_ratio` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 74> sum(coalesce(`sr`.`sr_return_quantity`, 0))), sum(<slot 75> sum(coalesce(`sts`.`ss_quantity`, 0))), sum(<slot 76> sum(coalesce(`sr`.`sr_return_amt`, 0))), sum(<slot 77> sum(coalesce(`sts`.`ss_net_paid`, 0)))\n" + 
				"  |  group by: <slot 73> `sts`.`ss_item_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(coalesce(<slot 228>, 0)), sum(coalesce(<slot 219>, 0)), sum(coalesce(<slot 229>, 0)), sum(coalesce(<slot 220>, 0))\n" + 
				"  |  group by: <slot 218>") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 208> = `sr`.`sr_ticket_number`)\n" + 
				"  |  equal join conjunct: (<slot 209> = `sr`.`sr_item_sk`)\n" + 
				"  |  other predicates: (<slot 327> > 10000)") && 
		explainStr.contains("other predicates: (<slot 327> > 10000)") && 
		explainStr.contains("vec output tuple id: 47") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 46") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`sts`.`ss_net_profit` > 1), (`sts`.`ss_net_paid` > 0), (`sts`.`ss_quantity` > 0)\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: store_returns(store_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001), (`d_moy` = 12)") && 
		explainStr.contains("order by: <slot 185> `return_ratio` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 44> sum(coalesce(`cr`.`cr_return_quantity`, 0))), sum(<slot 45> sum(coalesce(`cs`.`cs_quantity`, 0))), sum(<slot 46> sum(coalesce(`cr`.`cr_return_amount`, 0))), sum(<slot 47> sum(coalesce(`cs`.`cs_net_paid`, 0)))\n" + 
				"  |  group by: <slot 43> `cs`.`cs_item_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(coalesce(<slot 174>, 0)), sum(coalesce(<slot 165>, 0)), sum(coalesce(<slot 175>, 0)), sum(coalesce(<slot 166>, 0))\n" + 
				"  |  group by: <slot 164>") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 154> = `cr`.`cr_order_number`)\n" + 
				"  |  equal join conjunct: (<slot 155> = `cr`.`cr_item_sk`)\n" + 
				"  |  other predicates: (<slot 305> > 10000)") && 
		explainStr.contains("other predicates: (<slot 305> > 10000)") && 
		explainStr.contains("vec output tuple id: 37") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 36") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`cs`.`cs_net_profit` > 1), (`cs`.`cs_net_paid` > 0), (`cs`.`cs_quantity` > 0)\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `cs_sold_date_sk`") && 
		explainStr.contains("TABLE: catalog_returns(catalog_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001), (`d_moy` = 12)") && 
		explainStr.contains("order by: <slot 131> `return_ratio` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 14> sum(coalesce(`wr`.`wr_return_quantity`, 0))), sum(<slot 15> sum(coalesce(`ws`.`ws_quantity`, 0))), sum(<slot 16> sum(coalesce(`wr`.`wr_return_amt`, 0))), sum(<slot 17> sum(coalesce(`ws`.`ws_net_paid`, 0)))\n" + 
				"  |  group by: <slot 13> `ws`.`ws_item_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(coalesce(<slot 120>, 0)), sum(coalesce(<slot 111>, 0)), sum(coalesce(<slot 121>, 0)), sum(coalesce(<slot 112>, 0))\n" + 
				"  |  group by: <slot 110>") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 100> = `wr`.`wr_order_number`)\n" + 
				"  |  equal join conjunct: (<slot 101> = `wr`.`wr_item_sk`)\n" + 
				"  |  other predicates: (<slot 283> > 10000)") && 
		explainStr.contains("other predicates: (<slot 283> > 10000)") && 
		explainStr.contains("vec output tuple id: 27") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 26") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ws`.`ws_net_profit` > 1), (`ws`.`ws_net_paid` > 0), (`ws`.`ws_quantity` > 0)\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ws_sold_date_sk`") && 
		explainStr.contains("TABLE: web_returns(web_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001), (`d_moy` = 12)") 
            
        }
    }
}