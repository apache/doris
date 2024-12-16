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
suite("q16_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q16 """
SELECT
  count(DISTINCT cs_order_number) 'order count'
, sum(cs_ext_ship_cost) 'total shipping cost'
, sum(cs_net_profit) 'total net profit'
FROM
  catalog_sales cs1
, date_dim
, customer_address
, call_center
WHERE (d_date BETWEEN CAST('2002-2-01' AS DATE) AND (CAST('2002-2-01' AS DATE) + INTERVAL  '60' DAY))
   AND (cs1.cs_ship_date_sk = d_date_sk)
   AND (cs1.cs_ship_addr_sk = ca_address_sk)
   AND (ca_state = 'GA')
   AND (cs1.cs_call_center_sk = cc_call_center_sk)
   AND (cc_county IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County'))
   AND (EXISTS (
   SELECT *
   FROM
     catalog_sales cs2
   WHERE (cs1.cs_order_number = cs2.cs_order_number)
      AND (cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
))
   AND (NOT (EXISTS (
   SELECT *
   FROM
     catalog_returns cr1
   WHERE (cs1.cs_order_number = cr1.cr_order_number)
)))
ORDER BY count(DISTINCT cs_order_number) ASC
LIMIT 100
"""
}
