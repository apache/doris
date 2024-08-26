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
suite("q94_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q94 """
SELECT
  count(DISTINCT ws_order_number) 'order count'
, sum(ws_ext_ship_cost) 'total shipping cost'
, sum(ws_net_profit) 'total net profit'
FROM
  web_sales ws1
, date_dim
, customer_address
, web_site
WHERE (d_date BETWEEN CAST('1999-2-01' AS DATE) AND (CAST('1999-2-01' AS DATE) + INTERVAL  '60' DAY))
   AND (ws1.ws_ship_date_sk = d_date_sk)
   AND (ws1.ws_ship_addr_sk = ca_address_sk)
   AND (ca_state = 'IL')
   AND (ws1.ws_web_site_sk = web_site_sk)
   AND (web_company_name = 'pri')
   AND (EXISTS (
   SELECT *
   FROM
     web_sales ws2
   WHERE (ws1.ws_order_number = ws2.ws_order_number)
      AND (ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
))
   AND (NOT (EXISTS (
   SELECT *
   FROM
     web_returns wr1
   WHERE (ws1.ws_order_number = wr1.wr_order_number)
)))
ORDER BY count(DISTINCT ws_order_number) ASC
LIMIT 100
"""
}
