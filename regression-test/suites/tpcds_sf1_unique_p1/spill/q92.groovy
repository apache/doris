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
suite("q92_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  multi_sql """
    use regression_test_tpcds_sf1_unique_p1;
    set enable_local_shuffle=false;
  """

  qt_q92 """
SELECT sum(ws_ext_discount_amt) 'Excess Discount Amount'
FROM
  web_sales
, item
, date_dim
WHERE (i_manufact_id = 350)
   AND (i_item_sk = ws_item_sk)
   AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
   AND (d_date_sk = ws_sold_date_sk)
   AND (ws_ext_discount_amt > (
      SELECT (CAST('1.3' AS DECIMAL(2,1)) * avg(ws_ext_discount_amt))
      FROM
        web_sales
      , date_dim
      WHERE (ws_item_sk = i_item_sk)
         AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
         AND (d_date_sk = ws_sold_date_sk)
   ))
ORDER BY sum(ws_ext_discount_amt) ASC
LIMIT 100
"""
}
