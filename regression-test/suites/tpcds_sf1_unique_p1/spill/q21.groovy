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
suite("q21_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q21 """
SELECT *
FROM
  (
   SELECT
     w_warehouse_name
   , i_item_id
   , sum((CASE WHEN (CAST(d_date AS DATE) < CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_before
   , sum((CASE WHEN (CAST(d_date AS DATE) >= CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_after
   FROM
     inventory
   , warehouse
   , item
   , date_dim
   WHERE (i_current_price BETWEEN CAST('0.99' AS DECIMAL(3,2)) AND CAST('1.49' AS DECIMAL(3,2)))
      AND (i_item_sk = inv_item_sk)
      AND (inv_warehouse_sk = w_warehouse_sk)
      AND (inv_date_sk = d_date_sk)
      AND (d_date BETWEEN (CAST('2000-03-11' AS DATE) - INTERVAL  '30' DAY) AND (CAST('2000-03-11' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY w_warehouse_name, i_item_id
)  x
WHERE ((CASE WHEN (inv_before > 0) THEN (CAST(inv_after AS DECIMAL(7,2)) / inv_before) ELSE null END) BETWEEN (CAST('2.00' AS DECIMAL(3,2)) / CAST('3.00' AS DECIMAL(3,2))) AND (CAST('3.00' AS DECIMAL(3,2)) / CAST('2.00' AS DECIMAL(3,2))))
ORDER BY w_warehouse_name ASC, i_item_id ASC
LIMIT 100
"""
}
