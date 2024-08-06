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
suite("q18_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q18 """
SELECT
  i_item_id
, ca_country
, ca_state
, ca_county
, avg(CAST(cs_quantity AS DECIMAL(12,2))) agg1
, avg(CAST(cs_list_price AS DECIMAL(12,2))) agg2
, avg(CAST(cs_coupon_amt AS DECIMAL(12,2))) agg3
, avg(CAST(cs_sales_price AS DECIMAL(12,2))) agg4
, avg(CAST(cs_net_profit AS DECIMAL(12,2))) agg5
, avg(CAST(c_birth_year AS DECIMAL(12,2))) agg6
, avg(CAST(cd1.cd_dep_count AS DECIMAL(12,2))) agg7
FROM
  catalog_sales
, customer_demographics cd1
, customer_demographics cd2
, customer
, customer_address
, date_dim
, item
WHERE (cs_sold_date_sk = d_date_sk)
   AND (cs_item_sk = i_item_sk)
   AND (cs_bill_cdemo_sk = cd1.cd_demo_sk)
   AND (cs_bill_customer_sk = c_customer_sk)
   AND (cd1.cd_gender = 'F')
   AND (cd1.cd_education_status = 'Unknown')
   AND (c_current_cdemo_sk = cd2.cd_demo_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND (c_birth_month IN (1, 6, 8, 9, 12, 2))
   AND (d_year = 1998)
   AND (ca_state IN ('MS', 'IN', 'ND', 'OK', 'NM', 'VA', 'MS'))
GROUP BY ROLLUP (i_item_id, ca_country, ca_state, ca_county)
ORDER BY ca_country ASC, ca_state ASC, ca_county ASC, i_item_id ASC
LIMIT 100
"""
}
