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
suite("q55_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q55 """
SELECT
  i_brand_id brand_id
, i_brand brand
, sum(ss_ext_sales_price) ext_price
FROM
  date_dim
, store_sales
, item
WHERE (d_date_sk = ss_sold_date_sk)
   AND (ss_item_sk = i_item_sk)
   AND (i_manager_id = 28)
   AND (d_moy = 11)
   AND (d_year = 1999)
GROUP BY i_brand, i_brand_id
ORDER BY ext_price DESC, i_brand_id ASC
LIMIT 100
"""
}
