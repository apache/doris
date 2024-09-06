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
suite("q93_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q93 """
SELECT
  ss_customer_sk
, sum(act_sales) sumsales
FROM
  (
   SELECT
     ss_item_sk
   , ss_ticket_number
   , ss_customer_sk
   , (CASE WHEN (sr_return_quantity IS NOT NULL) THEN ((ss_quantity - sr_return_quantity) * ss_sales_price) ELSE (ss_quantity * ss_sales_price) END) act_sales
   FROM
     store_sales
   LEFT JOIN store_returns ON (sr_item_sk = ss_item_sk)
      AND (sr_ticket_number = ss_ticket_number)
   , reason
   WHERE (sr_reason_sk = r_reason_sk)
      AND (r_reason_desc = 'reason 28')
)  t
GROUP BY ss_customer_sk
ORDER BY sumsales ASC, ss_customer_sk ASC
LIMIT 100
"""
}
