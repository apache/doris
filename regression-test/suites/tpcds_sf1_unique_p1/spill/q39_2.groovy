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
suite("q39_2_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q39_2 """
WITH
  inv AS (
   SELECT
     w_warehouse_name
   , w_warehouse_sk
   , i_item_sk
   , d_moy
   , stdev
   , mean
   , (CASE mean WHEN 0 THEN null ELSE (stdev / mean) END) cov
   FROM
     (
      SELECT
        w_warehouse_name
      , w_warehouse_sk
      , i_item_sk
      , d_moy
      , stddev_samp(inv_quantity_on_hand) stdev
      , avg(inv_quantity_on_hand) mean
      FROM
        inventory
      , item
      , warehouse
      , date_dim
      WHERE (inv_item_sk = i_item_sk)
         AND (inv_warehouse_sk = w_warehouse_sk)
         AND (inv_date_sk = d_date_sk)
         AND (d_year = 2001)
      GROUP BY w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy
   )  foo
   WHERE ((CASE mean WHEN 0 THEN 0 ELSE (stdev / mean) END) > 1)
)
SELECT
  inv1.w_warehouse_sk
, inv1.i_item_sk
, inv1.d_moy
, inv1.mean
, inv1.cov
, inv2.w_warehouse_sk
, inv2.i_item_sk
, inv2.d_moy
, inv2.mean
, inv2.cov
FROM
  inv inv1
, inv inv2
WHERE (inv1.i_item_sk = inv2.i_item_sk)
   AND (inv1.w_warehouse_sk = inv2.w_warehouse_sk)
   AND (inv1.d_moy = 1)
   AND (inv2.d_moy = (1 + 1))
   AND (inv1.cov > CAST('1.5' AS DECIMAL(2,1)))
ORDER BY inv1.w_warehouse_sk ASC, inv1.i_item_sk ASC, inv1.d_moy ASC, inv1.mean ASC, inv1.cov ASC, inv2.d_moy ASC, inv2.mean ASC, inv2.cov ASC
"""
}
