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
suite("q43_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q43 """
SELECT
  s_store_name
, s_store_id
, sum((CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE null END)) sun_sales
, sum((CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE null END)) mon_sales
, sum((CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE null END)) tue_sales
, sum((CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE null END)) wed_sales
, sum((CASE WHEN (d_day_name = 'Thursday') THEN ss_sales_price ELSE null END)) thu_sales
, sum((CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE null END)) fri_sales
, sum((CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE null END)) sat_sales
FROM
  date_dim
, store_sales
, store
WHERE (d_date_sk = ss_sold_date_sk)
   AND (s_store_sk = ss_store_sk)
   AND (s_gmt_offset = -5)
   AND (d_year = 2000)
GROUP BY s_store_name, s_store_id
ORDER BY s_store_name ASC, s_store_id ASC, sun_sales ASC, mon_sales ASC, tue_sales ASC, wed_sales ASC, thu_sales ASC, fri_sales ASC, sat_sales ASC
LIMIT 100
"""
}
