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
suite("q12_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpch_sf0_1_p1;
  """
  qt_q12 """
-- tables: orders,lineitem
SELECT
/*+SET_VAR(enable_force_spill=true, min_revocable_mem=1)*/
  l_shipmode,
  sum(CASE
      WHEN o_orderpriority = '1-URGENT'
           OR o_orderpriority = '2-HIGH'
        THEN 1
      ELSE 0
      END) AS high_line_count,
  sum(CASE
      WHEN o_orderpriority <> '1-URGENT'
           AND o_orderpriority <> '2-HIGH'
        THEN 1
      ELSE 0
      END) AS low_line_count
FROM
  orders,
  lineitem
WHERE
  o_orderkey = l_orderkey
  AND l_shipmode IN ('MAIL', 'SHIP')
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate >= DATE '1994-01-01'
  AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY
  l_shipmode
ORDER BY
  l_shipmode

"""
}