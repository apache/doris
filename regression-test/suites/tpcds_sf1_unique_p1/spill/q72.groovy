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
suite("q72_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
/*
  qt_q72 """
-- For ASAN build, it will take too long time to run q72, disable for now
SELECT
  i_item_desc
, w_warehouse_name
, d1.d_week_seq
, sum((CASE WHEN (p_promo_sk IS NULL) THEN 1 ELSE 0 END)) no_promo
, sum((CASE WHEN (p_promo_sk IS NOT NULL) THEN 1 ELSE 0 END)) promo
, count(*) total_cnt
FROM
  catalog_sales
INNER JOIN inventory ON (cs_item_sk = inv_item_sk)
INNER JOIN warehouse ON (w_warehouse_sk = inv_warehouse_sk)
INNER JOIN item ON (i_item_sk = cs_item_sk)
INNER JOIN customer_demographics ON (cs_bill_cdemo_sk = cd_demo_sk)
INNER JOIN household_demographics ON (cs_bill_hdemo_sk = hd_demo_sk)
INNER JOIN date_dim d1 ON (cs_sold_date_sk = d1.d_date_sk)
INNER JOIN date_dim d2 ON (inv_date_sk = d2.d_date_sk)
INNER JOIN date_dim d3 ON (cs_ship_date_sk = d3.d_date_sk)
LEFT JOIN promotion ON (cs_promo_sk = p_promo_sk)
LEFT JOIN catalog_returns ON (cr_item_sk = cs_item_sk)
   AND (cr_order_number = cs_order_number)
WHERE (d1.d_week_seq = d2.d_week_seq)
   AND (inv_quantity_on_hand < cs_quantity)
   AND (d3.d_date > (d1.d_date + INTERVAL  '5' DAY))
   AND (hd_buy_potential = '>10000')
   AND (d1.d_year = 1999)
   AND (cd_marital_status = 'D')
GROUP BY i_item_desc, w_warehouse_name, d1.d_week_seq
ORDER BY total_cnt DESC, i_item_desc ASC, w_warehouse_name ASC, d1.d_week_seq ASC
LIMIT 100
"""
*/
}
