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
suite("q90_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q90 """
SELECT (CAST(amc AS DECIMAL(15,4)) / CAST(pmc AS DECIMAL(15,4))) am_pm_ratio
FROM
  (
   SELECT count(*) amc
   FROM
     web_sales
   , household_demographics
   , time_dim
   , web_page
   WHERE (ws_sold_time_sk = time_dim.t_time_sk)
      AND (ws_ship_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ws_web_page_sk = web_page.wp_web_page_sk)
      AND (time_dim.t_hour BETWEEN 8 AND (8 + 1))
      AND (household_demographics.hd_dep_count = 6)
      AND (web_page.wp_char_count BETWEEN 5000 AND 5200)
)  at
, (
   SELECT count(*) pmc
   FROM
     web_sales
   , household_demographics
   , time_dim
   , web_page
   WHERE (ws_sold_time_sk = time_dim.t_time_sk)
      AND (ws_ship_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ws_web_page_sk = web_page.wp_web_page_sk)
      AND (time_dim.t_hour BETWEEN 19 AND (19 + 1))
      AND (household_demographics.hd_dep_count = 6)
      AND (web_page.wp_char_count BETWEEN 5000 AND 5200)
)  pt
ORDER BY am_pm_ratio ASC
LIMIT 100
"""
}
