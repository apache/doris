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
suite("q06_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q06 """
--- takes over 30 minutes on travis to complete
SELECT
  a.ca_state STATE
, count(*) cnt
FROM
  customer_address a
, customer c
, store_sales s
, date_dim d
, item i
WHERE (a.ca_address_sk = c.c_current_addr_sk)
   AND (c.c_customer_sk = s.ss_customer_sk)
   AND (s.ss_sold_date_sk = d.d_date_sk)
   AND (s.ss_item_sk = i.i_item_sk)
   AND (d.d_month_seq = (
      SELECT DISTINCT d_month_seq
      FROM
        date_dim
      WHERE (d_year = 2001)
         AND (d_moy = 1)
   ))
   AND (i.i_current_price > (CAST('1.2' AS DECIMAL(2,1)) * (
         SELECT avg(j.i_current_price)
         FROM
           item j
         WHERE (j.i_category = i.i_category)
      )))
GROUP BY a.ca_state
HAVING (count(*) >= 10)
ORDER BY cnt ASC, a.ca_state ASC
LIMIT 100
"""
}
