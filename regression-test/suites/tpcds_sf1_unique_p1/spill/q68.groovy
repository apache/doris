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
suite("q68_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q68 """
SELECT
  c_last_name
, c_first_name
, ca_city
, bought_city
, ss_ticket_number
, extended_price
, extended_tax
, list_price
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , ca_city bought_city
   , sum(ss_ext_sales_price) extended_price
   , sum(ss_ext_list_price) list_price
   , sum(ss_ext_tax) extended_tax
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   , customer_address
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (store_sales.ss_addr_sk = customer_address.ca_address_sk)
      AND (date_dim.d_dom BETWEEN 1 AND 2)
      AND ((household_demographics.hd_dep_count = 4)
         OR (household_demographics.hd_vehicle_count = 3))
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_city IN ('Midway'   , 'Fairview'))
   GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city
)  dn
, customer
, customer_address current_addr
WHERE (ss_customer_sk = c_customer_sk)
   AND (customer.c_current_addr_sk = current_addr.ca_address_sk)
   AND (current_addr.ca_city <> bought_city)
ORDER BY c_last_name ASC, ss_ticket_number ASC
LIMIT 100
"""
}
