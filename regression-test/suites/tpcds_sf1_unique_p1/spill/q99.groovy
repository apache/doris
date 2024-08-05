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
suite("q99_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q99 """
SELECT
  substr(w_warehouse_name, 1, 20)
, sm_type
, cc_name
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) <= 30) THEN 1 ELSE 0 END)) '30 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 30)
   AND ((cs_ship_date_sk - cs_sold_date_sk) <= 60) THEN 1 ELSE 0 END)) '31-60 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 60)
   AND ((cs_ship_date_sk - cs_sold_date_sk) <= 90) THEN 1 ELSE 0 END)) '61-90 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 90)
   AND ((cs_ship_date_sk - cs_sold_date_sk) <= 120) THEN 1 ELSE 0 END)) '91-120 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 120) THEN 1 ELSE 0 END)) '>120 days'
FROM
  catalog_sales
, warehouse
, ship_mode
, call_center
, date_dim
WHERE (d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (cs_ship_date_sk = d_date_sk)
   AND (cs_warehouse_sk = w_warehouse_sk)
   AND (cs_ship_mode_sk = sm_ship_mode_sk)
   AND (cs_call_center_sk = cc_call_center_sk)
GROUP BY substr(w_warehouse_name, 1, 20), sm_type, cc_name
ORDER BY substr(w_warehouse_name, 1, 20) ASC, sm_type ASC, cc_name ASC
LIMIT 100
"""
}
