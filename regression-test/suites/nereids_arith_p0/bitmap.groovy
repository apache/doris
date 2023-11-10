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

suite('bitmap') {
    sql 'use regression_test_nereids_arith_p0'
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    test {
        sql "select id from (select BITMAP_EMPTY() as c0 from expr_test) as ref0 where c0 = 1 order by id"
        exception "can not cast from origin type BITMAP to target type=DOUBLE"
        sql "select id from expr_test group by id having ktint in (select BITMAP_EMPTY() from expr_test) order by id"
        exception "Doris hll, bitmap, array, map, struct, jsonb, variant column must use with specific function,"
    }
}
