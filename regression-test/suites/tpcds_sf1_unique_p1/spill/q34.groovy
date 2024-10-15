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
suite("q34_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q34 """
SELECT
  c_last_name
, c_first_name
, c_salutation
, c_preferred_cust_flag
, ss_ticket_number
, cnt
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , count(*) cnt
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND ((date_dim.d_dom BETWEEN 1 AND 3)
         OR (date_dim.d_dom BETWEEN 25 AND 28))
      AND ((household_demographics.hd_buy_potential = '>10000')
         OR (household_demographics.hd_buy_potential = 'Unknown'))
      AND (household_demographics.hd_vehicle_count > 0)
      AND ((CASE WHEN (household_demographics.hd_vehicle_count > 0) THEN (CAST(household_demographics.hd_dep_count AS DECIMAL(7,2)) / household_demographics.hd_vehicle_count) ELSE null END) > CAST('1.2' AS DECIMAL(2,1)))
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_county IN ('Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'))
   GROUP BY ss_ticket_number, ss_customer_sk
)  dn
, customer
WHERE (ss_customer_sk = c_customer_sk)
   AND (cnt BETWEEN 15 AND 20)
ORDER BY c_last_name ASC, c_first_name ASC, c_salutation ASC, c_preferred_cust_flag DESC, ss_ticket_number ASC
"""
}
