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
suite("q37_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q37 """
SELECT
  i_item_id
, i_item_desc
, i_current_price
FROM
  item
, inventory
, date_dim
, catalog_sales
WHERE (i_current_price BETWEEN 68 AND (68 + 30))
   AND (inv_item_sk = i_item_sk)
   AND (d_date_sk = inv_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('2000-02-01' AS DATE) AND (CAST('2000-02-01' AS DATE) + INTERVAL  '60' DAY))
   AND (i_manufact_id IN (677, 940, 694, 808))
   AND (inv_quantity_on_hand BETWEEN 100 AND 500)
   AND (cs_item_sk = i_item_sk)
GROUP BY i_item_id, i_item_desc, i_current_price
ORDER BY i_item_id ASC
LIMIT 100
"""
}
