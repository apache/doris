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
suite("q74_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q74 """
WITH
  year_total AS (
   SELECT
     c_customer_id customer_id
   , c_first_name customer_first_name
   , c_last_name customer_last_name
   , d_year YEAR
   , sum(ss_net_paid) year_total
   , 's' sale_type
   FROM
     customer
   , store_sales
   , date_dim
   WHERE (c_customer_sk = ss_customer_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year IN (2001   , (2001 + 1)))
   GROUP BY c_customer_id, c_first_name, c_last_name, d_year
UNION ALL    SELECT
     c_customer_id customer_id
   , c_first_name customer_first_name
   , c_last_name customer_last_name
   , d_year YEAR
   , sum(ws_net_paid) year_total
   , 'w' sale_type
   FROM
     customer
   , web_sales
   , date_dim
   WHERE (c_customer_sk = ws_bill_customer_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year IN (2001   , (2001 + 1)))
   GROUP BY c_customer_id, c_first_name, c_last_name, d_year
)
SELECT
  t_s_secyear.customer_id
, t_s_secyear.customer_first_name
, t_s_secyear.customer_last_name
FROM
  year_total t_s_firstyear
, year_total t_s_secyear
, year_total t_w_firstyear
, year_total t_w_secyear
WHERE (t_s_secyear.customer_id = t_s_firstyear.customer_id)
   AND (t_s_firstyear.customer_id = t_w_secyear.customer_id)
   AND (t_s_firstyear.customer_id = t_w_firstyear.customer_id)
   AND (t_s_firstyear.sale_type = 's')
   AND (t_w_firstyear.sale_type = 'w')
   AND (t_s_secyear.sale_type = 's')
   AND (t_w_secyear.sale_type = 'w')
   AND (t_s_firstyear.year = 2001)
   AND (t_s_secyear.year = (2001 + 1))
   AND (t_w_firstyear.year = 2001)
   AND (t_w_secyear.year = (2001 + 1))
   AND (t_s_firstyear.year_total > 0)
   AND (t_w_firstyear.year_total > 0)
   AND ((CASE WHEN (t_w_firstyear.year_total > 0) THEN (t_w_secyear.year_total / t_w_firstyear.year_total) ELSE null END) > (CASE WHEN (t_s_firstyear.year_total > 0) THEN (t_s_secyear.year_total / t_s_firstyear.year_total) ELSE null END))
ORDER BY 1 ASC, 1 ASC, 1 ASC
LIMIT 100
"""
}
