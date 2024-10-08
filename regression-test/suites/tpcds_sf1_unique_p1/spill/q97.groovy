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
suite("q97_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q97 """
WITH
  ssci AS (
   SELECT
     ss_customer_sk customer_sk
   , ss_item_sk item_sk
   FROM
     store_sales
   , date_dim
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
   GROUP BY ss_customer_sk, ss_item_sk
)
, csci AS (
   SELECT
     cs_bill_customer_sk customer_sk
   , cs_item_sk item_sk
   FROM
     catalog_sales
   , date_dim
   WHERE (cs_sold_date_sk = d_date_sk)
      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
   GROUP BY cs_bill_customer_sk, cs_item_sk
)
SELECT
  sum((CASE WHEN (ssci.customer_sk IS NOT NULL)
   AND (csci.customer_sk IS NULL) THEN 1 ELSE 0 END)) store_only
, sum((CASE WHEN (ssci.customer_sk IS NULL)
   AND (csci.customer_sk IS NOT NULL) THEN 1 ELSE 0 END)) catalog_only
, sum((CASE WHEN (ssci.customer_sk IS NOT NULL)
   AND (csci.customer_sk IS NOT NULL) THEN 1 ELSE 0 END)) store_and_catalog
FROM
  ssci
FULL JOIN csci ON (ssci.customer_sk = csci.customer_sk)
   AND (ssci.item_sk = csci.item_sk)
LIMIT 100
"""
}
