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
suite("q27_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q27 """
SELECT
  i_item_id
, s_state
, GROUPING (s_state) g_state
, avg(ss_quantity) agg1
, avg(ss_list_price) agg2
, avg(ss_coupon_amt) agg3
, avg(ss_sales_price) agg4
FROM
  store_sales
, customer_demographics
, date_dim
, store
, item
WHERE (ss_sold_date_sk = d_date_sk)
   AND (ss_item_sk = i_item_sk)
   AND (ss_store_sk = s_store_sk)
   AND (ss_cdemo_sk = cd_demo_sk)
   AND (cd_gender = 'M')
   AND (cd_marital_status = 'S')
   AND (cd_education_status = 'College')
   AND (d_year = 2002)
   AND (s_state IN (
     'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'))
GROUP BY ROLLUP (i_item_id, s_state)
ORDER BY i_item_id ASC, s_state ASC
LIMIT 100
"""
}
