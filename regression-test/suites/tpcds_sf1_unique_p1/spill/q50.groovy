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
suite("q50_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q50 """
SELECT
  s_store_name
, s_company_id
, s_street_number
, s_street_name
, s_street_type
, s_suite_number
, s_city
, s_county
, s_state
, s_zip
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) <= 30) THEN 1 ELSE 0 END)) '30 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 30)
   AND ((sr_returned_date_sk - ss_sold_date_sk) <= 60) THEN 1 ELSE 0 END)) '31-60 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 60)
   AND ((sr_returned_date_sk - ss_sold_date_sk) <= 90) THEN 1 ELSE 0 END)) '61-90 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 90)
   AND ((sr_returned_date_sk - ss_sold_date_sk) <= 120) THEN 1 ELSE 0 END)) '91-120 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 120) THEN 1 ELSE 0 END)) '>120 days'
FROM
  store_sales
, store_returns
, store
, date_dim d1
, date_dim d2
WHERE (d2.d_year = 2001)
   AND (d2.d_moy = 8)
   AND (ss_ticket_number = sr_ticket_number)
   AND (ss_item_sk = sr_item_sk)
   AND (ss_sold_date_sk = d1.d_date_sk)
   AND (sr_returned_date_sk = d2.d_date_sk)
   AND (ss_customer_sk = sr_customer_sk)
   AND (ss_store_sk = s_store_sk)
GROUP BY s_store_name, s_company_id, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip
ORDER BY s_store_name ASC, s_company_id ASC, s_street_number ASC, s_street_name ASC, s_street_type ASC, s_suite_number ASC, s_city ASC, s_county ASC, s_state ASC, s_zip ASC
LIMIT 100
"""
}
