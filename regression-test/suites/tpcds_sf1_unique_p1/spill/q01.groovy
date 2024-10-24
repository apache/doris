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
suite("q01_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q01 """
WITH
  customer_total_return AS (
   SELECT
     sr_customer_sk ctr_customer_sk
   , sr_store_sk ctr_store_sk
   , sum(sr_return_amt) ctr_total_return
   FROM
     store_returns
   , date_dim
   WHERE (sr_returned_date_sk = d_date_sk)
      AND (d_year = 2000)
   GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id
FROM
  customer_total_return ctr1
, store
, customer
WHERE (ctr1.ctr_total_return > (
      SELECT (avg(ctr_total_return) * CAST('1.2' AS DECIMAL(2,1)))
      FROM
        customer_total_return ctr2
      WHERE (ctr1.ctr_store_sk = ctr2.ctr_store_sk)
   ))
   AND (s_store_sk = ctr1.ctr_store_sk)
   AND (s_state = 'TN')
   AND (ctr1.ctr_customer_sk = c_customer_sk)
ORDER BY c_customer_id ASC
LIMIT 100
"""
}
