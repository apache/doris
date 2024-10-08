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
suite("q02_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q02 """
WITH
  wscs AS (
   SELECT
     sold_date_sk
   , sales_price
   FROM
     (
      SELECT
        ws_sold_date_sk sold_date_sk
      , ws_ext_sales_price sales_price
      FROM
        web_sales
   ) x
UNION ALL (
      SELECT
        cs_sold_date_sk sold_date_sk
      , cs_ext_sales_price sales_price
      FROM
        catalog_sales
   ) )
, wswscs AS (
   SELECT
     d_week_seq
   , sum((CASE WHEN (d_day_name = 'Sunday') THEN sales_price ELSE null END)) sun_sales
   , sum((CASE WHEN (d_day_name = 'Monday') THEN sales_price ELSE null END)) mon_sales
   , sum((CASE WHEN (d_day_name = 'Tuesday') THEN sales_price ELSE null END)) tue_sales
   , sum((CASE WHEN (d_day_name = 'Wednesday') THEN sales_price ELSE null END)) wed_sales
   , sum((CASE WHEN (d_day_name = 'Thursday') THEN sales_price ELSE null END)) thu_sales
   , sum((CASE WHEN (d_day_name = 'Friday') THEN sales_price ELSE null END)) fri_sales
   , sum((CASE WHEN (d_day_name = 'Saturday') THEN sales_price ELSE null END)) sat_sales
   FROM
     wscs
   , date_dim
   WHERE (d_date_sk = sold_date_sk)
   GROUP BY d_week_seq
)
SELECT
  d_week_seq1
, round((sun_sales1 / sun_sales2), 2)
, round((mon_sales1 / mon_sales2), 2)
, round((tue_sales1 / tue_sales2), 2)
, round((wed_sales1 / wed_sales2), 2)
, round((thu_sales1 / thu_sales2), 2)
, round((fri_sales1 / fri_sales2), 2)
, round((sat_sales1 / sat_sales2), 2)
FROM
  (
   SELECT
     wswscs.d_week_seq d_week_seq1
   , sun_sales sun_sales1
   , mon_sales mon_sales1
   , tue_sales tue_sales1
   , wed_sales wed_sales1
   , thu_sales thu_sales1
   , fri_sales fri_sales1
   , sat_sales sat_sales1
   FROM
     wswscs
   , date_dim
   WHERE (date_dim.d_week_seq = wswscs.d_week_seq)
      AND (d_year = 2001)
)  y
, (
   SELECT
     wswscs.d_week_seq d_week_seq2
   , sun_sales sun_sales2
   , mon_sales mon_sales2
   , tue_sales tue_sales2
   , wed_sales wed_sales2
   , thu_sales thu_sales2
   , fri_sales fri_sales2
   , sat_sales sat_sales2
   FROM
     wswscs
   , date_dim
   WHERE (date_dim.d_week_seq = wswscs.d_week_seq)
      AND (d_year = (2001 + 1))
)  z
WHERE (d_week_seq1 = (d_week_seq2 - 53))
ORDER BY d_week_seq1 ASC
"""
}
