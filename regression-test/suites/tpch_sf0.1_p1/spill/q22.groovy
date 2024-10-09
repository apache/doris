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
suite("q22_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpch_sf0_1_p1;
  """
  qt_q22 """
-- tables: orders,customer
SELECT
/*+SET_VAR(enable_force_spill=true, min_revocable_mem=1)*/
  cntrycode,
  count(*)       AS numcust,
  sum(c_acctbal) AS totacctbal
FROM (
       SELECT
         substr(c_phone, 1, 2) AS cntrycode,
         c_acctbal
       FROM
         customer
       WHERE
         substr(c_phone, 1, 2) IN
         ('13', '31', '23', '29', '30', '18', '17')
         AND c_acctbal > (
           SELECT avg(c_acctbal)
           FROM
             customer
           WHERE
             c_acctbal > 0.00
             AND substr(c_phone, 1, 2) IN
                 ('13', '31', '23', '29', '30', '18', '17')
         )
         AND NOT exists(
           SELECT *
           FROM
             orders
           WHERE
             o_custkey = c_custkey
         )
     ) AS custsale
GROUP BY
  cntrycode
ORDER BY
  cntrycode
"""
}