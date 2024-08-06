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
suite("q19_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q19 """
SELECT
  i_brand_id brand_id
, i_brand brand
, i_manufact_id
, i_manufact
, sum(ss_ext_sales_price) ext_price
FROM
  date_dim
, store_sales
, item
, customer
, customer_address
, store
WHERE (d_date_sk = ss_sold_date_sk)
   AND (ss_item_sk = i_item_sk)
   AND (i_manager_id = 8)
   AND (d_moy = 11)
   AND (d_year = 1998)
   AND (ss_customer_sk = c_customer_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND (substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5))
   AND (ss_store_sk = s_store_sk)
GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact
ORDER BY ext_price DESC, i_brand ASC, i_brand_id ASC, i_manufact_id ASC, i_manufact ASC
LIMIT 100
"""
}
