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
suite("q25_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q25 """
SELECT
  i_item_id
, i_item_desc
, s_store_id
, s_store_name
, sum(ss_net_profit) store_sales_profit
, sum(sr_net_loss) store_returns_loss
, sum(cs_net_profit) catalog_sales_profit
FROM
  store_sales
, store_returns
, catalog_sales
, date_dim d1
, date_dim d2
, date_dim d3
, store
, item
WHERE (d1.d_moy = 4)
   AND (d1.d_year = 2001)
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (i_item_sk = ss_item_sk)
   AND (s_store_sk = ss_store_sk)
   AND (ss_customer_sk = sr_customer_sk)
   AND (ss_item_sk = sr_item_sk)
   AND (ss_ticket_number = sr_ticket_number)
   AND (sr_returned_date_sk = d2.d_date_sk)
   AND (d2.d_moy BETWEEN 4 AND 10)
   AND (d2.d_year = 2001)
   AND (sr_customer_sk = cs_bill_customer_sk)
   AND (sr_item_sk = cs_item_sk)
   AND (cs_sold_date_sk = d3.d_date_sk)
   AND (d3.d_moy BETWEEN 4 AND 10)
   AND (d3.d_year = 2001)
GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
ORDER BY i_item_id ASC, i_item_desc ASC, s_store_id ASC, s_store_name ASC
LIMIT 100
"""
}
