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
suite("q79_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q79 """
SELECT
  c_last_name
, c_first_name
, substr(s_city, 1, 30)
, ss_ticket_number
, amt
, profit
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , store.s_city
   , sum(ss_coupon_amt) amt
   , sum(ss_net_profit) profit
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND ((household_demographics.hd_dep_count = 6)
         OR (household_demographics.hd_vehicle_count > 2))
      AND (date_dim.d_dow = 1)
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_number_employees BETWEEN 200 AND 295)
   GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, store.s_city
)  ms
, customer
WHERE (ss_customer_sk = c_customer_sk)
ORDER BY c_last_name ASC, c_first_name ASC, substr(s_city, 1, 30) ASC, profit ASC, ss_ticket_number, amt
LIMIT 100
"""
}
