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
suite("q61_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q61 """
SELECT
  promotions
, total
, ((CAST(promotions AS DECIMAL(15,4)) / CAST(total AS DECIMAL(15,4))) * 100)
FROM
  (
   SELECT sum(ss_ext_sales_price) promotions
   FROM
     store_sales
   , store
   , promotion
   , date_dim
   , customer
   , customer_address
   , item
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND (ss_promo_sk = p_promo_sk)
      AND (ss_customer_sk = c_customer_sk)
      AND (ca_address_sk = c_current_addr_sk)
      AND (ss_item_sk = i_item_sk)
      AND (ca_gmt_offset = -5)
      AND (i_category = 'Jewelry')
      AND ((p_channel_dmail = 'Y')
         OR (p_channel_email = 'Y')
         OR (p_channel_tv = 'Y'))
      AND (s_gmt_offset = -5)
      AND (d_year = 1998)
      AND (d_moy = 11)
)  promotional_sales
, (
   SELECT sum(ss_ext_sales_price) total
   FROM
     store_sales
   , store
   , date_dim
   , customer
   , customer_address
   , item
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND (ss_customer_sk = c_customer_sk)
      AND (ca_address_sk = c_current_addr_sk)
      AND (ss_item_sk = i_item_sk)
      AND (ca_gmt_offset = -5)
      AND (i_category = 'Jewelry')
      AND (s_gmt_offset = -5)
      AND (d_year = 1998)
      AND (d_moy = 11)
)  all_sales
ORDER BY promotions ASC, total ASC
LIMIT 100
"""
}
