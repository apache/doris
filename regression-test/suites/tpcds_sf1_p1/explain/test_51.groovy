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

suite("test_regression_test_tpcds_sf1_p1_q51", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  web_v1 AS (
		   SELECT
		     ws_item_sk item_sk
		   , d_date
		   , sum(sum(ws_sales_price)) OVER (PARTITION BY ws_item_sk ORDER BY d_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cume_sales
		   FROM
		     web_sales
		   , date_dim
		   WHERE (ws_sold_date_sk = d_date_sk)
		      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
		      AND (ws_item_sk IS NOT NULL)
		   GROUP BY ws_item_sk, d_date
		)
		, store_v1 AS (
		   SELECT
		     ss_item_sk item_sk
		   , d_date
		   , sum(sum(ss_sales_price)) OVER (PARTITION BY ss_item_sk ORDER BY d_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cume_sales
		   FROM
		     store_sales
		   , date_dim
		   WHERE (ss_sold_date_sk = d_date_sk)
		      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
		      AND (ss_item_sk IS NOT NULL)
		   GROUP BY ss_item_sk, d_date
		)
		SELECT *
		FROM
		  (
		   SELECT
		     item_sk
		   , d_date
		   , web_sales
		   , store_sales
		   , max(web_sales) OVER (PARTITION BY item_sk ORDER BY d_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) web_cumulative
		   , max(store_sales) OVER (PARTITION BY item_sk ORDER BY d_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) store_cumulative
		   FROM
		     (
		      SELECT
		        (CASE WHEN (web.item_sk IS NOT NULL) THEN web.item_sk ELSE store.item_sk END) item_sk
		      , (CASE WHEN (web.d_date IS NOT NULL) THEN web.d_date ELSE store.d_date END) d_date
		      , web.cume_sales web_sales
		      , store.cume_sales store_sales
		      FROM
		        web_v1 web
		      FULL JOIN store_v1 store ON (web.item_sk = store.item_sk)
		         AND (web.d_date = store.d_date)
		   )  x
		)  y
		WHERE (web_cumulative > store_cumulative)
		ORDER BY item_sk ASC, d_date ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 38> `item_sk` ASC, <slot 39> `d_date` ASC") && 
		explainStr.contains("predicates: (<slot 84> > <slot 85>)") && 
		explainStr.contains("order by: (CASE WHEN (<slot 87> <slot 79> IS NOT NULL) THEN <slot 87> <slot 79> ELSE <slot 90> <slot 82> END) ASC NULLS FIRST") && 
		explainStr.contains("order by: <slot 92> `item_sk` ASC, <slot 93> `d_date` ASC") && 
		explainStr.contains("join op: FULL OUTER JOIN(PARTITIONED)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 51> <slot 6> `ws_item_sk` = <slot 68> <slot 19> `ss_item_sk`)\n" + 
				"  |  equal join conjunct: (<slot 52> <slot 7> `d_date` = <slot 69> <slot 20> `d_date`)") && 
		explainStr.contains("vec output tuple id: 22") && 
		explainStr.contains("output slot ids: 78 79 80 81 82 83 \n" + 
				"  |  hash output slot ids: 50 51 67 52 68 69 ") && 
		explainStr.contains("order by: <slot 69> <slot 20> `d_date` ASC NULLS FIRST") && 
		explainStr.contains("order by: <slot 68> <slot 19> `ss_item_sk` ASC, <slot 69> <slot 20> `d_date` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 21> sum(`ss_sales_price`))\n" + 
				"  |  group by: <slot 19> `ss_item_sk`, <slot 20> `d_date`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 62>)\n" + 
				"  |  group by: <slot 61>, <slot 64>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 18") && 
		explainStr.contains("output slot ids: 61 62 64 \n" + 
				"  |  hash output slot ids: 13 14 15 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ss_item_sk` IS NOT NULL)\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_month_seq` >= 1200, `d_month_seq` <= 1211") && 
		explainStr.contains("order by: <slot 52> <slot 7> `d_date` ASC NULLS FIRST") && 
		explainStr.contains("order by: <slot 51> <slot 6> `ws_item_sk` ASC, <slot 52> <slot 7> `d_date` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 8> sum(`ws_sales_price`))\n" + 
				"  |  group by: <slot 6> `ws_item_sk`, <slot 7> `d_date`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 45>)\n" + 
				"  |  group by: <slot 44>, <slot 47>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 44 45 47 \n" + 
				"  |  hash output slot ids: 0 1 2 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ws_item_sk` IS NOT NULL)\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ws_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_month_seq` >= 1200, `d_month_seq` <= 1211") 
            
        }
    }
}