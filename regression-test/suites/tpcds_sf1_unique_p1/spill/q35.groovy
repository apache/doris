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
suite("q35_spill") {
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
  qt_q35 """
SELECT
  ca_state
, cd_gender
, cd_marital_status
, cd_dep_count
, count(*) cnt1
, min(cd_dep_count)
, max(cd_dep_count)
, avg(cd_dep_count)
, cd_dep_employed_count
, count(*) cnt2
, min(cd_dep_employed_count)
, max(cd_dep_employed_count)
, avg(cd_dep_employed_count)
, cd_dep_college_count
, count(*) cnt3
, min(cd_dep_college_count)
, max(cd_dep_college_count)
, avg(cd_dep_college_count)
FROM
  customer c
, customer_address ca
, customer_demographics
WHERE (c.c_current_addr_sk = ca.ca_address_sk)
   AND (cd_demo_sk = c.c_current_cdemo_sk)
   AND (EXISTS (
   SELECT *
   FROM
     store_sales
   , date_dim
   WHERE (c.c_customer_sk = ss_customer_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = 2002)
      AND (d_qoy < 4)
))
   AND ((EXISTS (
      SELECT *
      FROM
        web_sales
      , date_dim
      WHERE (c.c_customer_sk = ws_bill_customer_sk)
         AND (ws_sold_date_sk = d_date_sk)
         AND (d_year = 2002)
         AND (d_qoy < 4)
   ))
      OR (EXISTS (
      SELECT *
      FROM
        catalog_sales
      , date_dim
      WHERE (c.c_customer_sk = cs_ship_customer_sk)
         AND (cs_sold_date_sk = d_date_sk)
         AND (d_year = 2002)
         AND (d_qoy < 4)
   )))
GROUP BY ca_state, cd_gender, cd_marital_status, cd_dep_count, cd_dep_employed_count, cd_dep_college_count
ORDER BY ca_state ASC, cd_gender ASC, cd_marital_status ASC, cd_dep_count ASC, cd_dep_employed_count ASC, cd_dep_college_count ASC
LIMIT 100
"""
*/
}
