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
suite("q54_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_ck_p1;
  """
  qt_q54 """
WITH
  my_customers AS (
   SELECT DISTINCT
     c_customer_sk
   , c_current_addr_sk
   FROM
     (
      SELECT
        cs_sold_date_sk sold_date_sk
      , cs_bill_customer_sk customer_sk
      , cs_item_sk item_sk
      FROM
        catalog_sales
UNION ALL       SELECT
        ws_sold_date_sk sold_date_sk
      , ws_bill_customer_sk customer_sk
      , ws_item_sk item_sk
      FROM
        web_sales
   )  cs_or_ws_sales
   , item
   , date_dim
   , customer
   WHERE (sold_date_sk = d_date_sk)
      AND (item_sk = i_item_sk)
      AND (i_category = 'Women')
      AND (i_class = 'maternity')
      AND (c_customer_sk = cs_or_ws_sales.customer_sk)
      AND (d_moy = 12)
      AND (d_year = 1998)
)
, my_revenue AS (
   SELECT
     c_customer_sk
   , sum(ss_ext_sales_price) revenue
   FROM
     my_customers
   , store_sales
   , customer_address
   , store
   , date_dim
   WHERE (c_current_addr_sk = ca_address_sk)
      AND (ca_county = s_county)
      AND (ca_state = s_state)
      AND (ss_sold_date_sk = d_date_sk)
      AND (c_customer_sk = ss_customer_sk)
      AND (d_month_seq BETWEEN (
      SELECT DISTINCT (d_month_seq + 1)
      FROM
        date_dim
      WHERE (d_year = 1998)
         AND (d_moy = 12)
   ) AND (
      SELECT DISTINCT (d_month_seq + 3)
      FROM
        date_dim
      WHERE (d_year = 1998)
         AND (d_moy = 12)
   ))
   GROUP BY c_customer_sk
)
, segments AS (
   SELECT CAST((revenue / 50) AS INTEGER) segment
   FROM
     my_revenue
)
SELECT
  segment
, count(*) num_customers
, (segment * 50) segment_base
FROM
  segments
GROUP BY segment
ORDER BY segment ASC, num_customers ASC
LIMIT 100
"""
}
