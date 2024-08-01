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
suite("q38_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_p1;
  """
  qt_q38 """
SELECT count(*)
FROM
  (
   SELECT DISTINCT
     c_last_name
   , c_first_name
   , d_date
   FROM
     store_sales
   , date_dim
   , customer
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_customer_sk = customer.c_customer_sk)
      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
INTERSECT    SELECT DISTINCT
     c_last_name
   , c_first_name
   , d_date
   FROM
     catalog_sales
   , date_dim
   , customer
   WHERE (catalog_sales.cs_sold_date_sk = date_dim.d_date_sk)
      AND (catalog_sales.cs_bill_customer_sk = customer.c_customer_sk)
      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
INTERSECT    SELECT DISTINCT
     c_last_name
   , c_first_name
   , d_date
   FROM
     web_sales
   , date_dim
   , customer
   WHERE (web_sales.ws_sold_date_sk = date_dim.d_date_sk)
      AND (web_sales.ws_bill_customer_sk = customer.c_customer_sk)
      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
)  hot_cust
LIMIT 100
"""
}
