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

suite("nereids_scalar_fn_5") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    sql "select year(kdtv2) from fn_test order by kdtv2"
    sql "select year(kdtm) from fn_test order by kdtm"
    sql "select year(kdtmv2s1) from fn_test order by kdtmv2s1"
    sql "select year_ceil(kdtm) from fn_test order by kdtm"
    sql "select year_ceil(kdtmv2s1) from fn_test order by kdtmv2s1"
    sql "select year_ceil(kdtv2) from fn_test order by kdtv2"
    sql "select year_ceil(kdtm, kdtm) from fn_test order by kdtm, kdtm"
    sql "select year_ceil(kdtm, kint) from fn_test order by kdtm, kint"
    sql "select year_ceil(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
    sql "select year_ceil(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
    sql "select year_ceil(kdtv2, kdtv2) from fn_test order by kdtv2, kdtv2"
    sql "select year_ceil(kdtv2, kint) from fn_test order by kdtv2, kint"
    sql "select year_ceil(kdtm, kint, kdtm) from fn_test order by kdtm, kint, kdtm"
    sql "select year_ceil(kdtmv2s1, kint, kdtmv2s1) from fn_test order by kdtmv2s1, kint, kdtmv2s1"
    sql "select year_ceil(kdtv2, kint, kdtv2) from fn_test order by kdtv2, kint, kdtv2"
    sql "select year_floor(kdtm) from fn_test order by kdtm"
    sql "select year_floor(kdtmv2s1) from fn_test order by kdtmv2s1"
    sql "select year_floor(kdtv2) from fn_test order by kdtv2"
    sql "select year_floor(kdtm, kdtm) from fn_test order by kdtm, kdtm"
    sql "select year_floor(kdtm, kint) from fn_test order by kdtm, kint"
    sql "select year_floor(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
    sql "select year_floor(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
    sql "select year_floor(kdtv2, kdtv2) from fn_test order by kdtv2, kdtv2"
    sql "select year_floor(kdtv2, kint) from fn_test order by kdtv2, kint"
    sql "select year_floor(kdtm, kint, kdtm) from fn_test order by kdtm, kint, kdtm"
    sql "select year_floor(kdtmv2s1, kint, kdtmv2s1) from fn_test order by kdtmv2s1, kint, kdtmv2s1"
    sql "select year_floor(kdtv2, kint, kdtv2) from fn_test order by kdtv2, kint, kdtv2"
    sql "select year_week(kdtm) from fn_test order by kdtm"
    sql "select year_week(kdtmv2s1) from fn_test order by kdtmv2s1"
    sql "select year_week(kdtv2) from fn_test order by kdtv2"
    sql "select year_week(kdtm, kint) from fn_test order by kdtm, kint"
    sql "select year_week(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
    sql "select year_week(kdtv2, kint) from fn_test order by kdtv2, kint"
    sql "select years_add(kdtm, kint) from fn_test order by kdtm, kint"
    sql "select years_add(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
    sql "select years_add(kdt, kint) from fn_test order by kdt, kint"
    sql "select years_add(kdtv2, kint) from fn_test order by kdtv2, kint"
    sql "select years_diff(kdtm, kdtm) from fn_test order by kdtm, kdtm"
    sql "select years_diff(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
    sql "select years_diff(kdtv2, kdtmv2s1) from fn_test order by kdtv2, kdtmv2s1"
    sql "select years_diff(kdtmv2s1, kdtv2) from fn_test order by kdtmv2s1, kdtv2"
    sql "select years_diff(kdtv2, kdtv2) from fn_test order by kdtv2, kdtv2"
    sql "select years_diff(kdtv2, kdtm) from fn_test order by kdtv2, kdtm"
    sql "select years_diff(kdtm, kdtv2) from fn_test order by kdtm, kdtv2"
    sql "select years_diff(kdtmv2s1, kdtm) from fn_test order by kdtmv2s1, kdtm"
    sql "select years_diff(kdtm, kdtmv2s1) from fn_test order by kdtm, kdtmv2s1"
    sql "select years_sub(kdtm, kint) from fn_test order by kdtm, kint"
    sql "select years_sub(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
    sql "select years_sub(kdt, kint) from fn_test order by kdt, kint"
    sql "select years_sub(kdtv2, kint) from fn_test order by kdtv2, kint"
}