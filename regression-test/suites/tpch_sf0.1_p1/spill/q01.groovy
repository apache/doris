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
suite("q01_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpch_sf0_1_p1;
  """
  qt_q01 """
-- tables: lineitem
SELECT
  l_returnflag,
  l_linestatus,
  sum(l_quantity)                                       AS sum_qty,
  sum(l_extendedprice)                                  AS sum_base_price,
  sum(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
  avg(l_quantity)                                       AS avg_qty,
  avg(l_extendedprice)                                  AS avg_price,
  avg(l_discount)                                       AS avg_disc,
  count(*)                                              AS count_order
FROM
  lineitem
WHERE
  l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY
l_returnflag,
l_linestatus
ORDER BY
l_returnflag,
l_linestatus
"""
}