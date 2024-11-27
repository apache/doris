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
suite("q76_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q76 """
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
}
