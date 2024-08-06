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
suite("q96_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q96 """
SELECT count(*)
FROM
  store_sales
, household_demographics
, time_dim
, store
WHERE (ss_sold_time_sk = time_dim.t_time_sk)
   AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
   AND (ss_store_sk = s_store_sk)
   AND (time_dim.t_hour = 20)
   AND (time_dim.t_minute >= 30)
   AND (household_demographics.hd_dep_count = 7)
   AND (store.s_store_name = 'ese')
ORDER BY count(*) ASC
LIMIT 100
"""
}
