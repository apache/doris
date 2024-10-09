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
suite("q65_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q65 """
SELECT
  s_store_name
, i_item_desc
, sc.revenue
, i_current_price
, i_wholesale_cost
, i_brand
FROM
  store
, item
, (
   SELECT
     ss_store_sk
   , avg(revenue) ave
   FROM
     (
      SELECT
        ss_store_sk
      , ss_item_sk
      , sum(ss_sales_price) revenue
      FROM
        store_sales
      , date_dim
      WHERE (ss_sold_date_sk = d_date_sk)
         AND (d_month_seq BETWEEN 1176 AND (1176 + 11))
      GROUP BY ss_store_sk, ss_item_sk
   )  sa
   GROUP BY ss_store_sk
)  sb
, (
   SELECT
     ss_store_sk
   , ss_item_sk
   , sum(ss_sales_price) revenue
   FROM
     store_sales
   , date_dim
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_month_seq BETWEEN 1176 AND (1176 + 11))
   GROUP BY ss_store_sk, ss_item_sk
)  sc
WHERE (sb.ss_store_sk = sc.ss_store_sk)
   AND (sc.revenue <= (CAST('0.1' AS DECIMAL(2,1)) * sb.ave))
   AND (s_store_sk = sc.ss_store_sk)
   AND (i_item_sk = sc.ss_item_sk)
ORDER BY s_store_name ASC, i_item_desc ASC
LIMIT 100
"""
}
