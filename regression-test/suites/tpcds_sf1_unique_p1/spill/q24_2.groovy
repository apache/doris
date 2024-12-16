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
suite("q24_2_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=500;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q24_2 """
WITH
  ssales AS (
   SELECT
     c_last_name
   , c_first_name
   , s_store_name
   , ca_state
   , s_state
   , i_color
   , i_current_price
   , i_manager_id
   , i_units
   , i_size
   , sum(ss_net_paid) netpaid
   FROM
     store_sales
   , store_returns
   , store
   , item
   , customer
   , customer_address
   WHERE (ss_ticket_number = sr_ticket_number)
      AND (ss_item_sk = sr_item_sk)
      AND (ss_customer_sk = c_customer_sk)
      AND (ss_item_sk = i_item_sk)
      AND (ss_store_sk = s_store_sk)
      AND (c_birth_country = upper(ca_country))
      AND (s_zip = ca_zip)
      AND (s_market_id = 8)
   GROUP BY c_last_name, c_first_name, s_store_name, ca_state, s_state, i_color, i_current_price, i_manager_id, i_units, i_size
)
SELECT
  c_last_name
, c_first_name
, s_store_name
, sum(netpaid) paid
FROM
  ssales
WHERE (i_color = 'chiffon')
GROUP BY c_last_name, c_first_name, s_store_name
HAVING (sum(netpaid) > (
      SELECT (CAST('0.05' AS DECIMAL(5,2)) * avg(netpaid))
      FROM
        ssales
   ))
ORDER BY c_last_name, c_first_name, s_store_name
"""
}
