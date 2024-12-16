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
suite("q08_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpch_sf0_1_p1;
  """
  qt_q08 """
-- tables: part,supplier,lineitem,orders,customer,nation,region
SELECT
/*+SET_VAR(enable_force_spill=true, min_revocable_mem=1)*/
  o_year,
  sum(CASE
      WHEN nation = 'BRAZIL'
        THEN volume
      ELSE 0
      END) / sum(volume) AS mkt_share
FROM (
       SELECT
         extract(YEAR FROM o_orderdate)     AS o_year,
         l_extendedprice * (1 - l_discount) AS volume,
         n2.n_name                          AS nation
       FROM
         part,
         supplier,
         lineitem,
         orders,
         customer,
         nation n1,
         nation n2,
         region
       WHERE
         p_partkey = l_partkey
         AND s_suppkey = l_suppkey
         AND l_orderkey = o_orderkey
         AND o_custkey = c_custkey
         AND c_nationkey = n1.n_nationkey
         AND n1.n_regionkey = r_regionkey
         AND r_name = 'AMERICA'
         AND s_nationkey = n2.n_nationkey
         AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
         AND p_type = 'ECONOMY ANODIZED STEEL'
     ) AS all_nations
GROUP BY
  o_year
ORDER BY
  o_year

"""
}