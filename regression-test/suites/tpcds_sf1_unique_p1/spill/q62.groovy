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
suite("q62_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q62 """
SELECT
  substr(w_warehouse_name, 1, 20)
, sm_type
, web_name
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) <= 30) THEN 1 ELSE 0 END)) '30 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 30)
   AND ((ws_ship_date_sk - ws_sold_date_sk) <= 60) THEN 1 ELSE 0 END)) '31-60 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 60)
   AND ((ws_ship_date_sk - ws_sold_date_sk) <= 90) THEN 1 ELSE 0 END)) '61-90 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 90)
   AND ((ws_ship_date_sk - ws_sold_date_sk) <= 120) THEN 1 ELSE 0 END)) '91-120 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 120) THEN 1 ELSE 0 END)) '>120 days'
FROM
  web_sales
, warehouse
, ship_mode
, web_site
, date_dim
WHERE (d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (ws_ship_date_sk = d_date_sk)
   AND (ws_warehouse_sk = w_warehouse_sk)
   AND (ws_ship_mode_sk = sm_ship_mode_sk)
   AND (ws_web_site_sk = web_site_sk)
GROUP BY substr(w_warehouse_name, 1, 20), sm_type, web_name
ORDER BY substr(w_warehouse_name, 1, 20) ASC, sm_type ASC, web_name ASC
LIMIT 100
"""
}
