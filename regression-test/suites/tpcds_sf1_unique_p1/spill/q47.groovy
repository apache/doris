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
suite("q47_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=100;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q47 """
WITH
  v1 AS (
   SELECT
     i_category
   , i_brand
   , s_store_name
   , s_company_name
   , d_year
   , d_moy
   , sum(ss_sales_price) sum_sales
   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name, d_year) avg_monthly_sales
   , rank() OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name ORDER BY d_year ASC, d_moy ASC) rn
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND ((d_year = 1999)
         OR ((d_year = (1999 - 1))
            AND (d_moy = 12))
         OR ((d_year = (1999 + 1))
            AND (d_moy = 1)))
   GROUP BY i_category, i_brand, s_store_name, s_company_name, d_year, d_moy
)
, v2 AS (
   SELECT
     v1.i_category
   , v1.i_brand
   , v1.s_store_name
   , v1.s_company_name
   , v1.d_year
   , v1.d_moy
   , v1.avg_monthly_sales
   , v1.sum_sales
   , v1_lag.sum_sales psum
   , v1_lead.sum_sales nsum
   FROM
     v1
   , v1 v1_lag
   , v1 v1_lead
   WHERE (v1.i_category = v1_lag.i_category)
      AND (v1.i_category = v1_lead.i_category)
      AND (v1.i_brand = v1_lag.i_brand)
      AND (v1.i_brand = v1_lead.i_brand)
      AND (v1.s_store_name = v1_lag.s_store_name)
      AND (v1.s_store_name = v1_lead.s_store_name)
      AND (v1.s_company_name = v1_lag.s_company_name)
      AND (v1.s_company_name = v1_lead.s_company_name)
      AND (v1.rn = (v1_lag.rn + 1))
      AND (v1.rn = (v1_lead.rn - 1))
)
SELECT *
FROM
  v2
WHERE (d_year = 1999)
   AND (avg_monthly_sales > 0)
   AND ((CASE WHEN (avg_monthly_sales > 0) THEN (abs((sum_sales - avg_monthly_sales)) / avg_monthly_sales) ELSE null END) > CAST('0.1' AS DECIMAL(2,1)))
ORDER BY (sum_sales - avg_monthly_sales) ASC, 3 ASC
LIMIT 100
"""
}
