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
suite("q03_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q03 """
SELECT
  dt.d_year
, item.i_brand_id brand_id
, item.i_brand brand
, sum(ss_ext_sales_price) sum_agg
FROM
  date_dim dt
, store_sales
, item
WHERE (dt.d_date_sk = store_sales.ss_sold_date_sk)
   AND (store_sales.ss_item_sk = item.i_item_sk)
   AND (item.i_manufact_id = 128)
   AND (dt.d_moy = 11)
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year ASC, sum_agg DESC, brand_id ASC
LIMIT 100
"""
}
