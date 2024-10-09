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
suite("q86_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q86 """
SELECT
  sum(ws_net_paid) total_sum
, i_category
, i_class
, (GROUPING (i_category) + GROUPING (i_class)) lochierarchy
, rank() OVER (PARTITION BY (GROUPING (i_category) + GROUPING (i_class)), (CASE WHEN (GROUPING (i_class) = 0) THEN i_category END) ORDER BY sum(ws_net_paid) DESC) rank_within_parent
FROM
  web_sales
, date_dim d1
, item
WHERE (d1.d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (d1.d_date_sk = ws_sold_date_sk)
   AND (i_item_sk = ws_item_sk)
GROUP BY ROLLUP (i_category, i_class)
ORDER BY lochierarchy DESC, (CASE WHEN (lochierarchy = 0) THEN i_category END) ASC, rank_within_parent ASC
LIMIT 100
"""
}
