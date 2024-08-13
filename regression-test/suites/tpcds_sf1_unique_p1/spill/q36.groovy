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
suite("q36_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q36 """
SELECT
  (sum(ss_net_profit) / sum(ss_ext_sales_price)) gross_margin
, i_category
, i_class
, (GROUPING (i_category) + GROUPING (i_class)) lochierarchy
, rank() OVER (PARTITION BY (GROUPING (i_category) + GROUPING (i_class)), (CASE WHEN (GROUPING (i_class) = 0) THEN i_category END) ORDER BY (sum(ss_net_profit) / sum(ss_ext_sales_price)) ASC) rank_within_parent
FROM
  store_sales
, date_dim d1
, item
, store
WHERE (d1.d_year = 2001)
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (i_item_sk = ss_item_sk)
   AND (s_store_sk = ss_store_sk)
   AND (s_state IN (
     'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'))
GROUP BY ROLLUP (i_category, i_class)
ORDER BY lochierarchy DESC, (CASE WHEN (lochierarchy = 0) THEN i_category END) ASC, rank_within_parent ASC, i_category, i_class
LIMIT 100
"""
}
