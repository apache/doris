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
suite("q09_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q09 """
SELECT
  (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 1 AND 20)
   ) > 74129) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 20)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 20)
) END) bucket1
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 21 AND 40)
   ) > 122840) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 40)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 40)
) END) bucket2
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 41 AND 60)
   ) > 56580) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 41 AND 60)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 41 AND 60)
) END) bucket3
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 61 AND 80)
   ) > 10097) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 61 AND 80)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 61 AND 80)
) END) bucket4
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 81 AND 100)
   ) > 165306) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 81 AND 100)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 81 AND 100)
) END) bucket5
FROM
  reason
WHERE (r_reason_sk = 1)
"""
}
