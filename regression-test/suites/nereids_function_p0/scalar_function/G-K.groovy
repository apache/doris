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

    sql "select get_json_double(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
    sql "select get_json_double(kstr, kstr) from fn_test order by kstr, kstr"
    sql "select get_json_int(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
    sql "select get_json_int(kstr, kstr) from fn_test order by kstr, kstr"
    sql "select get_json_string(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
    sql "select get_json_string(kstr, kstr) from fn_test order by kstr, kstr"
    sql "select greatest(ktint) from fn_test order by ktint"
    sql "select greatest(ksint) from fn_test order by ksint"
    sql "select greatest(kint) from fn_test order by kint"
    sql "select greatest(kbint) from fn_test order by kbint"
    sql "select greatest(klint) from fn_test order by klint"
    sql "select greatest(kfloat) from fn_test order by kfloat"
    sql "select greatest(kdbl) from fn_test order by kdbl"
    sql "select greatest(kdcmls1) from fn_test order by kdcmls1"
    sql "select greatest(kdtm) from fn_test order by kdtm"
    sql "select greatest(kdtmv2s1) from fn_test order by kdtmv2s1"
    sql "select greatest(kvchrs1) from fn_test order by kvchrs1"
    sql "select greatest(kstr) from fn_test order by kstr"
    sql "select hex(kbint) from fn_test order by kbint"
    sql "select hex(kvchrs1) from fn_test order by kvchrs1"
    sql "select hex(kstr) from fn_test order by kstr"
// function hll_cardinality(hll) is unsupported for the test suite.
    sql "select hll_empty() from fn_test"
    sql "select hll_hash(kvchrs1) from fn_test order by kvchrs1"
    sql "select hll_hash(kstr) from fn_test order by kstr"
    sql "select hour(kdtm) from fn_test order by kdtm"
    sql "select hour(kdtmv2s1) from fn_test order by kdtmv2s1"
    sql "select hour(kdtv2) from fn_test order by kdtv2"
    sql "select hour_ceil(kdtm) from fn_test order by kdtm"
    sql "select hour_ceil(kdtmv2s1) from fn_test order by kdtmv2s1"
    // sql "select hour_ceil(kdtv2) from fn_test order by kdtv2"
    sql "select hour_ceil(kdtm, kdtm) from fn_test order by kdtm, kdtm"
    sql "select hour_ceil(kdtm, kint) from fn_test order by kdtm, kint"
    sql "select hour_ceil(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
    sql "select hour_ceil(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
    // sql "select hour_ceil(kdtv2, kdtv2) from fn_test order by kdtv2, kdtv2"
    // sql "select hour_ceil(kdtv2, kint) from fn_test order by kdtv2, kint"
    sql "select hour_ceil(kdtm, kint, kdtm) from fn_test order by kdtm, kint, kdtm"
    sql "select hour_ceil(kdtmv2s1, kint, kdtmv2s1) from fn_test order by kdtmv2s1, kint, kdtmv2s1"
    // sql "select hour_ceil(kdtv2, kint, kdtv2) from fn_test order by kdtv2, kint, kdtv2"
    sql "select hour_floor(kdtm) from fn_test order by kdtm"
    sql "select hour_floor(kdtmv2s1) from fn_test order by kdtmv2s1"
    // sql "select hour_floor(kdtv2) from fn_test order by kdtv2"
    sql "select hour_floor(kdtm, kdtm) from fn_test order by kdtm, kdtm"
    sql "select hour_floor(kdtm, kint) from fn_test order by kdtm, kint"
    sql "select hour_floor(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
    sql "select hour_floor(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
    // sql "select hour_floor(kdtv2, kdtv2) from fn_test order by kdtv2, kdtv2"
    // sql "select hour_floor(kdtv2, kint) from fn_test order by kdtv2, kint"
    sql "select hour_floor(kdtm, kint, kdtm) from fn_test order by kdtm, kint, kdtm"
    sql "select hour_floor(kdtmv2s1, kint, kdtmv2s1) from fn_test order by kdtmv2s1, kint, kdtmv2s1"
    // sql "select hour_floor(kdtv2, kint, kdtv2) from fn_test order by kdtv2, kint, kdtv2"
    // sql "select hours_add(kdtm, kint) from fn_test order by kdtm, kint"
    // sql "select hours_add(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
    // sql "select hours_add(kdt, kint) from fn_test order by kdt, kint"
    // sql "select hours_add(kdtv2, kint) from fn_test order by kdtv2, kint"
    sql "select hours_diff(kdtm, kdtm) from fn_test order by kdtm, kdtm"
    sql "select hours_diff(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
    sql "select hours_diff(kdtv2, kdtmv2s1) from fn_test order by kdtv2, kdtmv2s1"
    sql "select hours_diff(kdtmv2s1, kdtv2) from fn_test order by kdtmv2s1, kdtv2"
    sql "select hours_diff(kdtv2, kdtv2) from fn_test order by kdtv2, kdtv2"
    sql "select hours_diff(kdtv2, kdtm) from fn_test order by kdtv2, kdtm"
    sql "select hours_diff(kdtm, kdtv2) from fn_test order by kdtm, kdtv2"
    sql "select hours_diff(kdtmv2s1, kdtm) from fn_test order by kdtmv2s1, kdtm"
    sql "select hours_diff(kdtm, kdtmv2s1) from fn_test order by kdtm, kdtmv2s1"
    // sql "select hours_sub(kdtm, kint) from fn_test order by kdtm, kint"
    // sql "select hours_sub(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
    // sql "select hours_sub(kdt, kint) from fn_test order by kdt, kint"
    // sql "select hours_sub(kdtv2, kint) from fn_test order by kdtv2, kint"
    sql "select initcap(kvchrs1) from fn_test order by kvchrs1"
    sql "select instr(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
    sql "select instr(kstr, kstr) from fn_test order by kstr, kstr"
}