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
suite("q31_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q31 """
WITH
  ss AS (
   SELECT
     ca_county
   , d_qoy
   , d_year
   , sum(ss_ext_sales_price) store_sales
   FROM
     store_sales
   , date_dim
   , customer_address
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (ss_addr_sk = ca_address_sk)
   GROUP BY ca_county, d_qoy, d_year
)
, ws AS (
   SELECT
     ca_county
   , d_qoy
   , d_year
   , sum(ws_ext_sales_price) web_sales
   FROM
     web_sales
   , date_dim
   , customer_address
   WHERE (ws_sold_date_sk = d_date_sk)
      AND (ws_bill_addr_sk = ca_address_sk)
   GROUP BY ca_county, d_qoy, d_year
)
SELECT
  ss1.ca_county
, ss1.d_year
, (ws2.web_sales / ws1.web_sales) web_q1_q2_increase
, (ss2.store_sales / ss1.store_sales) store_q1_q2_increase
, (ws3.web_sales / ws2.web_sales) web_q2_q3_increase
, (ss3.store_sales / ss2.store_sales) store_q2_q3_increase
FROM
  ss ss1
, ss ss2
, ss ss3
, ws ws1
, ws ws2
, ws ws3
WHERE (ss1.d_qoy = 1)
   AND (ss1.d_year = 2000)
   AND (ss1.ca_county = ss2.ca_county)
   AND (ss2.d_qoy = 2)
   AND (ss2.d_year = 2000)
   AND (ss2.ca_county = ss3.ca_county)
   AND (ss3.d_qoy = 3)
   AND (ss3.d_year = 2000)
   AND (ss1.ca_county = ws1.ca_county)
   AND (ws1.d_qoy = 1)
   AND (ws1.d_year = 2000)
   AND (ws1.ca_county = ws2.ca_county)
   AND (ws2.d_qoy = 2)
   AND (ws2.d_year = 2000)
   AND (ws1.ca_county = ws3.ca_county)
   AND (ws3.d_qoy = 3)
   AND (ws3.d_year = 2000)
   AND ((CASE WHEN (ws1.web_sales > 0) THEN (CAST(ws2.web_sales AS DECIMAL(21,3)) / ws1.web_sales) ELSE null END) > (CASE WHEN (ss1.store_sales > 0) THEN (CAST(ss2.store_sales AS DECIMAL(21,3)) / ss1.store_sales) ELSE null END))
   AND ((CASE WHEN (ws2.web_sales > 0) THEN (CAST(ws3.web_sales AS DECIMAL(21,3)) / ws2.web_sales) ELSE null END) > (CASE WHEN (ss2.store_sales > 0) THEN (CAST(ss3.store_sales AS DECIMAL(21,3)) / ss2.store_sales) ELSE null END))
ORDER BY ss1.ca_county ASC
"""
}
