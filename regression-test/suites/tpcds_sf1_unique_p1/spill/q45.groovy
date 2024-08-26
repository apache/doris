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
suite("q45_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
/*
  qt_q45 """
SELECT
  ca_zip
, ca_city
, sum(ws_sales_price)
FROM
  web_sales
, customer
, customer_address
, date_dim
, item
WHERE (ws_bill_customer_sk = c_customer_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND (ws_item_sk = i_item_sk)
   AND ((substr(ca_zip, 1, 5) IN ('85669'   , '86197'   , '88274'   , '83405'   , '86475'   , '85392'   , '85460'   , '80348'   , '81792'))
      OR (i_item_id IN (
      SELECT i_item_id
      FROM
        item
      WHERE (i_item_sk IN (2      , 3      , 5      , 7      , 11      , 13      , 17      , 19      , 23      , 29))
   )))
   AND (ws_sold_date_sk = d_date_sk)
   AND (d_qoy = 2)
   AND (d_year = 2001)
GROUP BY ca_zip, ca_city
ORDER BY ca_zip ASC, ca_city ASC
LIMIT 100
"""
*/
}
