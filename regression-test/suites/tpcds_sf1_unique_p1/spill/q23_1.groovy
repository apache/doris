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
suite("q23_1_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q23_1 """
WITH
  frequent_ss_items AS (
   SELECT
     substr(i_item_desc, 1, 30) itemdesc
   , i_item_sk item_sk
   , d_date solddate
   , count(*) cnt
   FROM
     store_sales
   , date_dim
   , item
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (ss_item_sk = i_item_sk)
      AND (d_year IN (2000   , (2000 + 1)   , (2000 + 2)   , (2000 + 3)))
   GROUP BY substr(i_item_desc, 1, 30), i_item_sk, d_date
   HAVING (count(*) > 4)
)
, max_store_sales AS (
   SELECT max(csales) tpcds_cmax
   FROM
     (
      SELECT
        c_customer_sk
      , sum((ss_quantity * ss_sales_price)) csales
      FROM
        store_sales
      , customer
      , date_dim
      WHERE (ss_customer_sk = c_customer_sk)
         AND (ss_sold_date_sk = d_date_sk)
         AND (d_year IN (2000      , (2000 + 1)      , (2000 + 2)      , (2000 + 3)))
      GROUP BY c_customer_sk
   ) x
)
, best_ss_customer AS (
   SELECT
     c_customer_sk
   , sum((ss_quantity * ss_sales_price)) ssales
   FROM
     store_sales
   , customer
   WHERE (ss_customer_sk = c_customer_sk)
   GROUP BY c_customer_sk
   HAVING (sum((ss_quantity * ss_sales_price)) > ((50 / CAST('100.0' AS DECIMAL(5,1))) * (
            SELECT *
            FROM
              max_store_sales
         )))
)
SELECT sum(sales)
FROM
  (
   SELECT (cs_quantity * cs_list_price) sales
   FROM
     catalog_sales
   , date_dim
   WHERE (d_year = 2000)
      AND (d_moy = 2)
      AND (cs_sold_date_sk = d_date_sk)
      AND (cs_item_sk IN (
      SELECT item_sk
      FROM
        frequent_ss_items
   ))
      AND (cs_bill_customer_sk IN (
      SELECT c_customer_sk
      FROM
        best_ss_customer
   ))
UNION ALL    SELECT (ws_quantity * ws_list_price) sales
   FROM
     web_sales
   , date_dim
   WHERE (d_year = 2000)
      AND (d_moy = 2)
      AND (ws_sold_date_sk = d_date_sk)
      AND (ws_item_sk IN (
      SELECT item_sk
      FROM
        frequent_ss_items
   ))
      AND (ws_bill_customer_sk IN (
      SELECT c_customer_sk
      FROM
        best_ss_customer
   ))
) y
LIMIT 100
"""
}
