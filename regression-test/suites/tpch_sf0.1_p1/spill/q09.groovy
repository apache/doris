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
    use regression_test_tpch_sf0_1_p1;
  """
  qt_q09 """
-- tables: part,supplier,lineitem,partsupp,orders,nation
SELECT
  nation,
  o_year,
  sum(amount) AS sum_profit
FROM (
       SELECT
         n_name                                                          AS nation,
         extract(YEAR FROM o_orderdate)                                  AS o_year,
         l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
       FROM
         part,
         supplier,
         lineitem,
         partsupp,
         orders,
         nation
       WHERE
         s_suppkey = l_suppkey
         AND ps_suppkey = l_suppkey
         AND ps_partkey = l_partkey
         AND p_partkey = l_partkey
         AND o_orderkey = l_orderkey
         AND s_nationkey = n_nationkey
         AND p_name LIKE '%green%'
     ) AS profit
GROUP BY
  nation,
  o_year
ORDER BY
  nation,
  o_year DESC

"""
}