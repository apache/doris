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
suite("q32_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q32 """
SELECT sum(cs_ext_discount_amt) 'excess discount amount'
FROM
  catalog_sales
, item
, date_dim
WHERE (i_manufact_id = 977)
   AND (i_item_sk = cs_item_sk)
   AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
   AND (d_date_sk = cs_sold_date_sk)
   AND (cs_ext_discount_amt > (
      SELECT (CAST('1.3' AS DECIMAL(2,1)) * avg(cs_ext_discount_amt))
      FROM
        catalog_sales
      , date_dim
      WHERE (cs_item_sk = i_item_sk)
         AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
         AND (d_date_sk = cs_sold_date_sk)
   ))
LIMIT 100
"""
}
