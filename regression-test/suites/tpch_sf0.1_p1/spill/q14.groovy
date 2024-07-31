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
suite("q14_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set min_revocable_mem=1;
  """
  sql """
    use regression_test_tpch_sf0_1_p1;
  """
  qt_q14 """
-- tables: lineitem,part
SELECT
/*+SET_VAR(enable_force_spill=true, min_revocable_mem=1)*/
  100.00 * sum(CASE
                    WHEN p_type LIKE 'PROMO%'
                      THEN l_extendedprice * (1 - l_discount)
                    ELSE 0
                    END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
  lineitem,
  part
WHERE
  l_partkey = p_partkey
  AND l_shipdate >= DATE '1995-09-01'
  AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH

"""
}