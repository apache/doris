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
suite("q12_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q12 """
SELECT
  i_item_id
, i_item_desc
, i_category
, i_class
, i_current_price
, sum(ws_ext_sales_price) itemrevenue
, ((sum(ws_ext_sales_price) * 100) / sum(sum(ws_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
FROM
  web_sales
, item
, date_dim
WHERE (ws_item_sk = i_item_sk)
   AND (i_category IN ('Sports', 'Books', 'Home'))
   AND (ws_sold_date_sk = d_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category ASC, i_class ASC, i_item_id ASC, i_item_desc ASC, revenueratio ASC
LIMIT 100
"""
}
