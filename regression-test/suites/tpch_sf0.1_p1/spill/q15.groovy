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
    use regression_test_tpch_sf0_1_p1;
  """
  qt_q15 """
SELECT
/*+SET_VAR(enable_force_spill=true, min_revocable_mem=1)*/
  s_suppkey,
  s_name,
  s_address,
  s_phone,
  total_revenue
FROM
  supplier,
  revenue1
WHERE
  s_suppkey = supplier_no
  AND total_revenue = (
    SELECT max(total_revenue)
    FROM
      revenue1
  )
ORDER BY
  s_suppkey;
"""
}
