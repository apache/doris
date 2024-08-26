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
suite("q07_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpch_sf0_1_p1;
  """
  qt_q07 """
-- tables: supplier,lineitem,orders,customer,nation
SELECT
/*+SET_VAR(enable_force_spill=true, min_revocable_mem=1)*/
  supp_nation,
  cust_nation,
  l_year,
  sum(volume) AS revenue
FROM (
       SELECT
         n1.n_name                          AS supp_nation,
         n2.n_name                          AS cust_nation,
         extract(YEAR FROM l_shipdate)      AS l_year,
         l_extendedprice * (1 - l_discount) AS volume
       FROM
         supplier,
         lineitem,
         orders,
         customer,
         nation n1,
         nation n2
       WHERE
         s_suppkey = l_suppkey
         AND o_orderkey = l_orderkey
         AND c_custkey = o_custkey
         AND s_nationkey = n1.n_nationkey
         AND c_nationkey = n2.n_nationkey
         AND (
           (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
           OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
         )
         AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
     ) AS shipping
GROUP BY
  supp_nation,
  cust_nation,
  l_year
ORDER BY
  supp_nation,
  cust_nation,
  l_year

"""
}