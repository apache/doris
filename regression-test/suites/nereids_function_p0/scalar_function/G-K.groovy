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

suite("nereids_scalar_fn_2") {
    sql "use regression_test_nereids_function_p0"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    qt_sql "select get_json_double(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
    qt_sql "select get_json_double(kstr, kstr) from fn_test order by kstr, kstr"
    qt_sql "select get_json_int(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
    qt_sql "select get_json_int(kstr, kstr) from fn_test order by kstr, kstr"
    qt_sql "select get_json_string(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
    qt_sql "select get_json_string(kstr, kstr) from fn_test order by kstr, kstr"
    qt_sql "select greatest(ktint) from fn_test order by ktint"
    qt_sql "select greatest(ksint) from fn_test order by ksint"
    qt_sql "select greatest(kint) from fn_test order by kint"
    qt_sql "select greatest(kbint) from fn_test order by kbint"
    qt_sql "select greatest(klint) from fn_test order by klint"
    qt_sql "select greatest(kfloat) from fn_test order by kfloat"
    qt_sql "select greatest(kdbl) from fn_test order by kdbl"
    qt_sql "select greatest(kdcmls1) from fn_test order by kdcmls1"
    qt_sql "select greatest(kdtm) from fn_test order by kdtm"
    qt_sql "select greatest(kdtmv2s1) from fn_test order by kdtmv2s1"
    qt_sql "select greatest(kvchrs1) from fn_test order by kvchrs1"
    qt_sql "select greatest(kstr) from fn_test order by kstr"
    qt_sql "select hex(kbint) from fn_test order by kbint"
    qt_sql "select hex(kvchrs1) from fn_test order by kvchrs1"
    qt_sql "select hex(kstr) from fn_test order by kstr"
// function hll_cardinality(hll) is unsupported for the test suite.
    qt_sql "select hll_empty() from fn_test"
    qt_sql "select hll_hash(kvchrs1) from fn_test order by kvchrs1"
    qt_sql "select hll_hash(kstr) from fn_test order by kstr"
    qt_sql "select hour(kdtm) from fn_test order by kdtm"
    qt_sql "select hour(kdtmv2s1) from fn_test order by kdtmv2s1"
    qt_sql "select hour(kdtv2) from fn_test order by kdtv2"
    qt_sql "select hour_ceil(kdtm) from fn_test order by kdtm"
    qt_sql "select hour_ceil(kdtmv2s1) from fn_test order by kdtmv2s1"
    // qt_sql "select hour_ceil(kdtv2) from fn_test order by kdtv2"
    qt_sql "select hour_ceil(kdtm, kdtm) from fn_test order by kdtm, kdtm"
    qt_sql "select hour_ceil(kdtm, kint) from fn_test order by kdtm, kint"
    qt_sql "select hour_ceil(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
    qt_sql "select hour_ceil(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
    // qt_sql "select hour_ceil(kdtv2, kdtv2) from fn_test order by kdtv2, kdtv2"
    // qt_sql "select hour_ceil(kdtv2, kint) from fn_test order by kdtv2, kint"
    qt_sql "select hour_ceil(kdtm, kint, kdtm) from fn_test order by kdtm, kint, kdtm"
    qt_sql "select hour_ceil(kdtmv2s1, kint, kdtmv2s1) from fn_test order by kdtmv2s1, kint, kdtmv2s1"
    // qt_sql "select hour_ceil(kdtv2, kint, kdtv2) from fn_test order by kdtv2, kint, kdtv2"
    qt_sql "select hour_floor(kdtm) from fn_test order by kdtm"
    qt_sql "select hour_floor(kdtmv2s1) from fn_test order by kdtmv2s1"
    // qt_sql "select hour_floor(kdtv2) from fn_test order by kdtv2"
    qt_sql "select hour_floor(kdtm, kdtm) from fn_test order by kdtm, kdtm"
    qt_sql "select hour_floor(kdtm, kint) from fn_test order by kdtm, kint"
    qt_sql "select hour_floor(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
    qt_sql "select hour_floor(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
    // qt_sql "select hour_floor(kdtv2, kdtv2) from fn_test order by kdtv2, kdtv2"
    // qt_sql "select hour_floor(kdtv2, kint) from fn_test order by kdtv2, kint"
    qt_sql "select hour_floor(kdtm, kint, kdtm) from fn_test order by kdtm, kint, kdtm"
    qt_sql "select hour_floor(kdtmv2s1, kint, kdtmv2s1) from fn_test order by kdtmv2s1, kint, kdtmv2s1"
    // qt_sql "select hour_floor(kdtv2, kint, kdtv2) from fn_test order by kdtv2, kint, kdtv2"
    // qt_sql "select hours_add(kdtm, kint) from fn_test order by kdtm, kint"
    // qt_sql "select hours_add(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
    // qt_sql "select hours_add(kdt, kint) from fn_test order by kdt, kint"
    // qt_sql "select hours_add(kdtv2, kint) from fn_test order by kdtv2, kint"
    qt_sql "select hours_diff(kdtm, kdtm) from fn_test order by kdtm, kdtm"
    qt_sql "select hours_diff(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
    qt_sql "select hours_diff(kdtv2, kdtmv2s1) from fn_test order by kdtv2, kdtmv2s1"
    qt_sql "select hours_diff(kdtmv2s1, kdtv2) from fn_test order by kdtmv2s1, kdtv2"
    qt_sql "select hours_diff(kdtv2, kdtv2) from fn_test order by kdtv2, kdtv2"
    qt_sql "select hours_diff(kdtv2, kdtm) from fn_test order by kdtv2, kdtm"
    qt_sql "select hours_diff(kdtm, kdtv2) from fn_test order by kdtm, kdtv2"
    qt_sql "select hours_diff(kdtmv2s1, kdtm) from fn_test order by kdtmv2s1, kdtm"
    qt_sql "select hours_diff(kdtm, kdtmv2s1) from fn_test order by kdtm, kdtmv2s1"
    // qt_sql "select hours_sub(kdtm, kint) from fn_test order by kdtm, kint"
    // qt_sql "select hours_sub(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
    // qt_sql "select hours_sub(kdt, kint) from fn_test order by kdt, kint"
    // qt_sql "select hours_sub(kdtv2, kint) from fn_test order by kdtv2, kint"
    qt_sql "select initcap(kvchrs1) from fn_test order by kvchrs1"
    qt_sql "select instr(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
    qt_sql "select instr(kstr, kstr) from fn_test order by kstr, kstr"
}