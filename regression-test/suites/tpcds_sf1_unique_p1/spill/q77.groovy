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
suite("q77_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q77 """
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
}
