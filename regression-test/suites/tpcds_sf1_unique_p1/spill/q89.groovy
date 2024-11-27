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
suite("q89_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q89 """
SELECT *
FROM
  (
   SELECT
     i_category
   , i_class
   , i_brand
   , s_store_name
   , s_company_name
   , d_moy
   , sum(ss_sales_price) sum_sales
   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name) avg_monthly_sales
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND (d_year IN (1999))
      AND (((i_category IN ('Books'         , 'Electronics'         , 'Sports'))
            AND (i_class IN ('computers'         , 'stereo'         , 'football')))
         OR ((i_category IN ('Men'         , 'Jewelry'         , 'Women'))
            AND (i_class IN ('shirts'         , 'birdal'         , 'dresses'))))
   GROUP BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy
)  tmp1
WHERE ((CASE WHEN (avg_monthly_sales <> 0) THEN (abs((sum_sales - avg_monthly_sales)) / avg_monthly_sales) ELSE null END) > CAST('0.1' AS DECIMAL(2,1)))
ORDER BY (sum_sales - avg_monthly_sales) ASC, s_store_name ASC
LIMIT 100
"""
}
