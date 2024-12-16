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
suite("q22_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q22 """
SELECT
  i_product_name
, i_brand
, i_class
, i_category
, avg(inv_quantity_on_hand) qoh
FROM
  inventory
, date_dim
, item
WHERE (inv_date_sk = d_date_sk)
   AND (inv_item_sk = i_item_sk)
   AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
GROUP BY ROLLUP (i_product_name, i_brand, i_class, i_category)
ORDER BY qoh ASC, i_product_name ASC, i_brand ASC, i_class ASC, i_category ASC
LIMIT 100
"""
}
