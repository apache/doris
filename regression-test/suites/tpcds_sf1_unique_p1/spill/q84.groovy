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
suite("q84_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q84 """
SELECT
  c_customer_id customer_id
, concat(concat(c_last_name, ', '), c_first_name) customername
FROM
  customer
, customer_address
, customer_demographics
, household_demographics
, income_band
, store_returns
WHERE (ca_city = 'Edgewood')
   AND (c_current_addr_sk = ca_address_sk)
   AND (ib_lower_bound >= 38128)
   AND (ib_upper_bound <= (38128 + 50000))
   AND (ib_income_band_sk = hd_income_band_sk)
   AND (cd_demo_sk = c_current_cdemo_sk)
   AND (hd_demo_sk = c_current_hdemo_sk)
   AND (sr_cdemo_sk = cd_demo_sk)
ORDER BY c_customer_id ASC
LIMIT 100
"""
}
