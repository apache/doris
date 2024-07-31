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
suite("q26_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q26 """
SELECT
  i_item_id
, avg(cs_quantity) agg1
, avg(cs_list_price) agg2
, avg(cs_coupon_amt) agg3
, avg(cs_sales_price) agg4
FROM
  catalog_sales
, customer_demographics
, date_dim
, item
, promotion
WHERE (cs_sold_date_sk = d_date_sk)
   AND (cs_item_sk = i_item_sk)
   AND (cs_bill_cdemo_sk = cd_demo_sk)
   AND (cs_promo_sk = p_promo_sk)
   AND (cd_gender = 'M')
   AND (cd_marital_status = 'S')
   AND (cd_education_status = 'College')
   AND ((p_channel_email = 'N')
      OR (p_channel_event = 'N'))
   AND (d_year = 2000)
GROUP BY i_item_id
ORDER BY i_item_id ASC
LIMIT 100
"""
}
