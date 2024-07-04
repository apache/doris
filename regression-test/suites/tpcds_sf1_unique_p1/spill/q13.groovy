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
suite("q13_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q13 """
SELECT
  avg(ss_quantity)
, avg(ss_ext_sales_price)
, avg(ss_ext_wholesale_cost)
, sum(ss_ext_wholesale_cost)
FROM
  store_sales
, store
, customer_demographics
, household_demographics
, customer_address
, date_dim
WHERE (s_store_sk = ss_store_sk)
   AND (ss_sold_date_sk = d_date_sk)
   AND (d_year = 2001)
   AND (((ss_hdemo_sk = hd_demo_sk)
         AND (cd_demo_sk = ss_cdemo_sk)
         AND (cd_marital_status = 'M')
         AND (cd_education_status = 'Advanced Degree')
         AND (ss_sales_price BETWEEN CAST('100.00' AS DECIMAL(5,2)) AND CAST('150.00' AS DECIMAL(5,2)))
         AND (hd_dep_count = 3))
      OR ((ss_hdemo_sk = hd_demo_sk)
         AND (cd_demo_sk = ss_cdemo_sk)
         AND (cd_marital_status = 'S')
         AND (cd_education_status = 'College')
         AND (ss_sales_price BETWEEN CAST('50.00' AS DECIMAL(5,2)) AND CAST('100.00' AS DECIMAL(5,2)))
         AND (hd_dep_count = 1))
      OR ((ss_hdemo_sk = hd_demo_sk)
         AND (cd_demo_sk = ss_cdemo_sk)
         AND (cd_marital_status = 'W')
         AND (cd_education_status = '2 yr Degree')
         AND (ss_sales_price BETWEEN CAST('150.00' AS DECIMAL(5,2)) AND CAST('200.00' AS DECIMAL(5,2)))
         AND (hd_dep_count = 1)))
   AND (((ss_addr_sk = ca_address_sk)
         AND (ca_country = 'United States')
         AND (ca_state IN ('TX'      , 'OH'      , 'TX'))
         AND (ss_net_profit BETWEEN 100 AND 200))
      OR ((ss_addr_sk = ca_address_sk)
         AND (ca_country = 'United States')
         AND (ca_state IN ('OR'      , 'NM'      , 'KY'))
         AND (ss_net_profit BETWEEN 150 AND 300))
      OR ((ss_addr_sk = ca_address_sk)
         AND (ca_country = 'United States')
         AND (ca_state IN ('VA'      , 'TX'      , 'MS'))
         AND (ss_net_profit BETWEEN 50 AND 250)))
"""
}
