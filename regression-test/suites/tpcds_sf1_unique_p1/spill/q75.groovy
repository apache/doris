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
suite("q75_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=100;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q75 """
WITH
  all_sales AS (
   SELECT
     d_year
   , i_brand_id
   , i_class_id
   , i_category_id
   , i_manufact_id
   , sum(sales_cnt) sales_cnt
   , sum(sales_amt) sales_amt
   FROM
     (
      SELECT
        d_year
      , i_brand_id
      , i_class_id
      , i_category_id
      , i_manufact_id
      , (cs_quantity - COALESCE(cr_return_quantity, 0)) sales_cnt
      , (cs_ext_sales_price - COALESCE(cr_return_amount, CAST('0.0' AS DECIMAL(2,1)))) sales_amt
      FROM
        catalog_sales
      INNER JOIN item ON (i_item_sk = cs_item_sk)
      INNER JOIN date_dim ON (d_date_sk = cs_sold_date_sk)
      LEFT JOIN catalog_returns ON (cs_order_number = cr_order_number)
         AND (cs_item_sk = cr_item_sk)
      WHERE (i_category = 'Books')
UNION       SELECT
        d_year
      , i_brand_id
      , i_class_id
      , i_category_id
      , i_manufact_id
      , (ss_quantity - COALESCE(sr_return_quantity, 0)) sales_cnt
      , (ss_ext_sales_price - COALESCE(sr_return_amt, CAST('0.0' AS DECIMAL(2,1)))) sales_amt
      FROM
        store_sales
      INNER JOIN item ON (i_item_sk = ss_item_sk)
      INNER JOIN date_dim ON (d_date_sk = ss_sold_date_sk)
      LEFT JOIN store_returns ON (ss_ticket_number = sr_ticket_number)
         AND (ss_item_sk = sr_item_sk)
      WHERE (i_category = 'Books')
UNION       SELECT
        d_year
      , i_brand_id
      , i_class_id
      , i_category_id
      , i_manufact_id
      , (ws_quantity - COALESCE(wr_return_quantity, 0)) sales_cnt
      , (ws_ext_sales_price - COALESCE(wr_return_amt, CAST('0.0' AS DECIMAL(2,1)))) sales_amt
      FROM
        web_sales
      INNER JOIN item ON (i_item_sk = ws_item_sk)
      INNER JOIN date_dim ON (d_date_sk = ws_sold_date_sk)
      LEFT JOIN web_returns ON (ws_order_number = wr_order_number)
         AND (ws_item_sk = wr_item_sk)
      WHERE (i_category = 'Books')
   )  sales_detail
   GROUP BY d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id
)
SELECT
  prev_yr.d_year prev_year
, curr_yr.d_year year
, curr_yr.i_brand_id
, curr_yr.i_class_id
, curr_yr.i_category_id
, curr_yr.i_manufact_id
, prev_yr.sales_cnt prev_yr_cnt
, curr_yr.sales_cnt curr_yr_cnt
, (curr_yr.sales_cnt - prev_yr.sales_cnt) sales_cnt_diff
, (curr_yr.sales_amt - prev_yr.sales_amt) sales_amt_diff
FROM
  all_sales curr_yr
, all_sales prev_yr
WHERE (curr_yr.i_brand_id = prev_yr.i_brand_id)
   AND (curr_yr.i_class_id = prev_yr.i_class_id)
   AND (curr_yr.i_category_id = prev_yr.i_category_id)
   AND (curr_yr.i_manufact_id = prev_yr.i_manufact_id)
   AND (curr_yr.d_year = 2002)
   AND (prev_yr.d_year = (2002 - 1))
   AND ((CAST(curr_yr.sales_cnt AS DECIMAL(17,2)) / CAST(prev_yr.sales_cnt AS DECIMAL(17,2))) < CAST('0.9' AS DECIMAL(2,1)))
ORDER BY sales_cnt_diff ASC, sales_amt_diff ASC
LIMIT 100
"""
}
