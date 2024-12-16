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
suite("q71_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q71 """
SELECT
  i_brand_id brand_id
, i_brand brand
, t_hour
, t_minute
, sum(ext_price) ext_price
FROM
  item
, (
   SELECT
     ws_ext_sales_price ext_price
   , ws_sold_date_sk sold_date_sk
   , ws_item_sk sold_item_sk
   , ws_sold_time_sk time_sk
   FROM
     web_sales
   , date_dim
   WHERE (d_date_sk = ws_sold_date_sk)
      AND (d_moy = 11)
      AND (d_year = 1999)
UNION ALL    SELECT
     cs_ext_sales_price ext_price
   , cs_sold_date_sk sold_date_sk
   , cs_item_sk sold_item_sk
   , cs_sold_time_sk time_sk
   FROM
     catalog_sales
   , date_dim
   WHERE (d_date_sk = cs_sold_date_sk)
      AND (d_moy = 11)
      AND (d_year = 1999)
UNION ALL    SELECT
     ss_ext_sales_price ext_price
   , ss_sold_date_sk sold_date_sk
   , ss_item_sk sold_item_sk
   , ss_sold_time_sk time_sk
   FROM
     store_sales
   , date_dim
   WHERE (d_date_sk = ss_sold_date_sk)
      AND (d_moy = 11)
      AND (d_year = 1999)
)  tmp
, time_dim
WHERE (sold_item_sk = i_item_sk)
   AND (i_manager_id = 1)
   AND (time_sk = t_time_sk)
   AND ((t_meal_time = 'breakfast')
      OR (t_meal_time = 'dinner'))
GROUP BY i_brand, i_brand_id, t_hour, t_minute
ORDER BY ext_price DESC, i_brand_id ASC
"""
}
