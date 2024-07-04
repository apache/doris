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
suite("q40_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q40 """
SELECT
  w_state
, i_item_id
, sum((CASE WHEN (CAST(d_date AS DATE) < CAST('2000-03-11' AS DATE)) THEN (cs_sales_price - COALESCE(cr_refunded_cash, 0)) ELSE 0 END)) sales_before
, sum((CASE WHEN (CAST(d_date AS DATE) >= CAST('2000-03-11' AS DATE)) THEN (cs_sales_price - COALESCE(cr_refunded_cash, 0)) ELSE 0 END)) sales_after
FROM
  catalog_sales
LEFT JOIN catalog_returns ON (cs_order_number = cr_order_number)
   AND (cs_item_sk = cr_item_sk)
, warehouse
, item
, date_dim
WHERE (i_current_price BETWEEN CAST('0.99' AS DECIMAL(3,2)) AND CAST('1.49' AS DECIMAL(3,2)))
   AND (i_item_sk = cs_item_sk)
   AND (cs_warehouse_sk = w_warehouse_sk)
   AND (cs_sold_date_sk = d_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN (CAST('2000-03-11' AS DATE) - INTERVAL  '30' DAY) AND (CAST('2000-03-11' AS DATE) + INTERVAL  '30' DAY))
GROUP BY w_state, i_item_id
ORDER BY w_state ASC, i_item_id ASC
LIMIT 100
"""
}
