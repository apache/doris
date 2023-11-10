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

package infer_expr_name

suite("nereids_test_create_blocked") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    def queryResult = sql """
explain analyzed plan
select
  ca_state,
  cd_gender,
  cd_marital_status,
  cd_dep_count,
  count(*) cnt1,
  max(cd_dep_count),
  sum(cd_dep_count),
  max(cd_dep_count),
  cd_dep_employed_count,
  count(*) cnt2,
  max(cd_dep_employed_count),
  sum(cd_dep_employed_count),
  max(cd_dep_employed_count),
  cd_dep_college_count,
  count(*) cnt3,
  max(cd_dep_college_count),
  sum(cd_dep_college_count),
  max(cd_dep_college_count)
 from
  customer c,customer_address ca,customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  cd_demo_sk = c.c_current_cdemo_sk and
  exists (select *
          from store_sales,date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 2001 and
                d_qoy < 4) and
   (exists (select *
            from web_sales,date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = 2001 and
                  d_qoy < 4) or
    exists (select *
            from catalog_sales,date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = 2001 and
                  d_qoy < 4))
 group by ca_state,
          cd_gender,
          cd_marital_status,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 order by ca_state,
          cd_gender,
          cd_marital_status,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 limit 100;
"""

    def topPlan = queryResult[0][0].toString()
    assertTrue(topPlan.contains("LogicalResultSink"))
    assertTrue(topPlan.contains("ca_state"))
    assertTrue(topPlan.contains("cd_gender"))
    assertTrue(topPlan.contains("cd_marital_status"))
    assertTrue(topPlan.contains("cd_dep_count"))
    assertTrue(topPlan.contains("cnt1"))
    assertTrue(topPlan.contains("__max_5"))
    assertTrue(topPlan.contains("__sum_6"))
    assertTrue(topPlan.contains("__max_7"))
    assertTrue(topPlan.contains("cd_dep_employed_count"))
    assertTrue(topPlan.contains("cnt2"))
    assertTrue(topPlan.contains("__max_10"))
    assertTrue(topPlan.contains("__sum_11"))
    assertTrue(topPlan.contains("__max_12"))
    assertTrue(topPlan.contains("cd_dep_college_count"))
    assertTrue(topPlan.contains("cnt3"))
    assertTrue(topPlan.contains("__sum_16"))
    assertTrue(topPlan.contains("__max_17"))
}