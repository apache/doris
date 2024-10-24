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
suite("q59_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q59 """
WITH
  wss AS (
   SELECT
     d_week_seq
   , ss_store_sk
   , sum((CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE null END)) sun_sales
   , sum((CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE null END)) mon_sales
   , sum((CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE null END)) tue_sales
   , sum((CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE null END)) wed_sales
   , sum((CASE WHEN (d_day_name = 'Thursday ') THEN ss_sales_price ELSE null END)) thu_sales
   , sum((CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE null END)) fri_sales
   , sum((CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE null END)) sat_sales
   FROM
     store_sales
   , date_dim
   WHERE (d_date_sk = ss_sold_date_sk)
   GROUP BY d_week_seq, ss_store_sk
)
SELECT
  s_store_name1
, s_store_id1
, d_week_seq1
, (sun_sales1 / sun_sales2)
, (mon_sales1 / mon_sales2)
, (tue_sales1 / tue_sales2)
, (wed_sales1 / wed_sales2)
, (thu_sales1 / thu_sales2)
, (fri_sales1 / fri_sales2)
, (sat_sales1 / sat_sales2)
FROM
  (
   SELECT
     s_store_name s_store_name1
   , wss.d_week_seq d_week_seq1
   , s_store_id s_store_id1
   , sun_sales sun_sales1
   , mon_sales mon_sales1
   , tue_sales tue_sales1
   , wed_sales wed_sales1
   , thu_sales thu_sales1
   , fri_sales fri_sales1
   , sat_sales sat_sales1
   FROM
     wss
   , store
   , date_dim d
   WHERE (d.d_week_seq = wss.d_week_seq)
      AND (ss_store_sk = s_store_sk)
      AND (d_month_seq BETWEEN 1212 AND (1212 + 11))
)  y
, (
   SELECT
     s_store_name s_store_name2
   , wss.d_week_seq d_week_seq2
   , s_store_id s_store_id2
   , sun_sales sun_sales2
   , mon_sales mon_sales2
   , tue_sales tue_sales2
   , wed_sales wed_sales2
   , thu_sales thu_sales2
   , fri_sales fri_sales2
   , sat_sales sat_sales2
   FROM
     wss
   , store
   , date_dim d
   WHERE (d.d_week_seq = wss.d_week_seq)
      AND (ss_store_sk = s_store_sk)
      AND (d_month_seq BETWEEN (1212 + 12) AND (1212 + 23))
)  x
WHERE (s_store_id1 = s_store_id2)
   AND (d_week_seq1 = (d_week_seq2 - 52))
ORDER BY s_store_name1 ASC, s_store_id1 ASC, d_week_seq1 ASC
LIMIT 100
"""
}
