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
suite("q51_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q51 """
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
}
