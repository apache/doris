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
suite("q15_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q15 """
SELECT
  ca_zip
, sum(cs_sales_price)
FROM
  catalog_sales
, customer
, customer_address
, date_dim
WHERE (cs_bill_customer_sk = c_customer_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND ((substr(ca_zip, 1, 5) IN ('85669'   , '86197'   , '88274'   , '83405'   , '86475'   , '85392'   , '85460'   , '80348'   , '81792'))
      OR (ca_state IN ('CA'   , 'WA'   , 'GA'))
      OR (cs_sales_price > 500))
   AND (cs_sold_date_sk = d_date_sk)
   AND (d_qoy = 2)
   AND (d_year = 2001)
GROUP BY ca_zip
ORDER BY ca_zip ASC
LIMIT 100
"""
}
