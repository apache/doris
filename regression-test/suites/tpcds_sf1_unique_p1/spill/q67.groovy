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
suite("q67_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q67 """
SELECT *
FROM
  (
   SELECT
     i_category
   , i_class
   , i_brand
   , i_product_name
   , d_year
   , d_qoy
   , d_moy
   , s_store_id
   , sumsales
   , rank() OVER (PARTITION BY i_category ORDER BY sumsales DESC) rk
   FROM
     (
      SELECT
        i_category
      , i_class
      , i_brand
      , i_product_name
      , d_year
      , d_qoy
      , d_moy
      , s_store_id
      , sum(COALESCE((ss_sales_price * ss_quantity), 0)) sumsales
      FROM
        store_sales
      , date_dim
      , store
      , item
      WHERE (ss_sold_date_sk = d_date_sk)
         AND (ss_item_sk = i_item_sk)
         AND (ss_store_sk = s_store_sk)
         AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
      GROUP BY ROLLUP (i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id)
   )  dw1
)  dw2
WHERE (rk <= 100)
ORDER BY i_category ASC, i_class ASC, i_brand ASC, i_product_name ASC, d_year ASC, d_qoy ASC, d_moy ASC, s_store_id ASC, sumsales ASC, rk ASC
LIMIT 100
"""
}
