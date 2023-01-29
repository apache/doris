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

suite("nereids_scalar_fn_1") {
    sql "use regression_test_nereids_function_p0"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    qt_sql "select abs(kdbl) from fn_test order by kdbl"
    qt_sql "select abs(kfloat) from fn_test order by kfloat"
    qt_sql "select abs(klint) from fn_test order by klint"
    qt_sql "select abs(kbint) from fn_test order by kbint"
    qt_sql "select abs(ksint) from fn_test order by ksint"
    qt_sql "select abs(kint) from fn_test order by kint"
    qt_sql "select abs(ktint) from fn_test order by ktint"
    qt_sql "select abs(kdcmls1) from fn_test order by kdcmls1"
    // data out of function definition field
    // qt_sql "select acos(kdbl) from fn_test order by kdbl"
    sql "select aes_decrypt(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
    sql "select aes_decrypt(kstr, kstr) from fn_test order by kstr, kstr"
    sql "select aes_decrypt(kvchrs1, kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1, kvchrs1"
    sql "select aes_decrypt(kstr, kstr, kstr) from fn_test order by kstr, kstr, kstr"
    // cannot find function
    // sql "select aes_decrypt(kvchrs1, kvchrs1, kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1, kvchrs1, kvchrs1"
    // sql "select aes_decrypt(kstr, kstr, kstr, kstr) from fn_test order by kstr, kstr, kstr, kstr"
    sql "select aes_encrypt(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
    sql "select aes_encrypt(kstr, kstr) from fn_test order by kstr, kstr"
    sql "select aes_encrypt(kvchrs1, kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1, kvchrs1"
    sql "select aes_encrypt(kstr, kstr, kstr) from fn_test order by kstr, kstr, kstr"
    // cannot find function
    // sql "select aes_encrypt(kvchrs1, kvchrs1, kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1, kvchrs1, kvchrs1"
    // sql "select aes_encrypt(kstr, kstr, kstr, kstr) from fn_test order by kstr, kstr, kstr, kstr"
    qt_sql "select append_trailing_char_if_absent(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
    qt_sql "select append_trailing_char_if_absent(kstr, kstr) from fn_test order by kstr, kstr"
    qt_sql "select ascii(kvchrs1) from fn_test order by kvchrs1"
    qt_sql "select ascii(kstr) from fn_test order by kstr"
    // data out of function definition field
    // qt_sql "select asin(kdbl) from fn_test order by kdbl"
    qt_sql "select atan(kdbl) from fn_test order by kdbl"
    qt_sql "select bin(kbint) from fn_test order by kbint"
    qt_sql "select bit_length(kvchrs1) from fn_test order by kvchrs1"
    qt_sql "select bit_length(kstr) from fn_test order by kstr"
// function bitmap_and(bitmap, bitmap) is unsupported for the test suite.
// function bitmap_and_count(bitmap, bitmap) is unsupported for the test suite.
// function bitmap_and_not(bitmap, bitmap) is unsupported for the test suite.
// function bitmap_and_not_count(bitmap, bitmap) is unsupported for the test suite.
// function bitmap_contains(bitmap, bigint) is unsupported for the test suite.
// function bitmap_count(bitmap) is unsupported for the test suite.
    qt_sql "select bitmap_empty() from fn_test"
    qt_sql "select bitmap_from_string(kvchrs1) from fn_test order by kvchrs1"
    qt_sql "select bitmap_from_string(kstr) from fn_test order by kstr"
// function bitmap_has_all(bitmap, bitmap) is unsupported for the test suite.
// function bitmap_has_any(bitmap, bitmap) is unsupported for the test suite.
    qt_sql "select bitmap_hash(kvchrs1) from fn_test order by kvchrs1"
    qt_sql "select bitmap_hash(kstr) from fn_test order by kstr"
    qt_sql "select bitmap_hash64(kvchrs1) from fn_test order by kvchrs1"
    qt_sql "select bitmap_hash64(kstr) from fn_test order by kstr"
// function bitmap_max(bitmap) is unsupported for the test suite.
// function bitmap_min(bitmap) is unsupported for the test suite.
// function bitmap_not(bitmap, bitmap) is unsupported for the test suite.
// function bitmap_or(bitmap, bitmap) is unsupported for the test suite.
// function bitmap_or_count(bitmap, bitmap) is unsupported for the test suite.
// function bitmap_subset_in_range(bitmap, bigint, bigint) is unsupported for the test suite.
// function bitmap_subset_limit(bitmap, bigint, bigint) is unsupported for the test suite.
// function bitmap_to_string(bitmap) is unsupported for the test suite.
// function bitmap_xor(bitmap, bitmap) is unsupported for the test suite.
// function bitmap_xor_count(bitmap, bitmap) is unsupported for the test suite.
    qt_sql "select cbrt(kdbl) from fn_test order by kdbl"
    qt_sql "select ceil(kdbl) from fn_test order by kdbl"
    qt_sql "select ceiling(kdbl) from fn_test order by kdbl"
    qt_sql "select character_length(kvchrs1) from fn_test order by kvchrs1"
    qt_sql "select character_length(kstr) from fn_test order by kstr"
    qt_sql "select coalesce(kbool) from fn_test order by kbool"
    qt_sql "select coalesce(ktint) from fn_test order by ktint"
    qt_sql "select coalesce(ksint) from fn_test order by ksint"
    qt_sql "select coalesce(kint) from fn_test order by kint"
    qt_sql "select coalesce(kbint) from fn_test order by kbint"
    qt_sql "select coalesce(klint) from fn_test order by klint"
    qt_sql "select coalesce(kfloat) from fn_test order by kfloat"
    qt_sql "select coalesce(kdbl) from fn_test order by kdbl"
    qt_sql "select coalesce(kdtm) from fn_test order by kdtm"
    qt_sql "select coalesce(kdt) from fn_test order by kdt"
    qt_sql "select coalesce(kdtmv2s1) from fn_test order by kdtmv2s1"
    qt_sql "select coalesce(kdtv2) from fn_test order by kdtv2"
    qt_sql "select coalesce(kdcmls1) from fn_test order by kdcmls1"
// function coalesce(bitmap) is unsupported for the test suite.
    qt_sql "select coalesce(kvchrs1) from fn_test order by kvchrs1"
    qt_sql "select coalesce(kstr) from fn_test order by kstr"
    qt_sql "select concat(kvchrs1) from fn_test order by kvchrs1"
    qt_sql "select concat(kstr) from fn_test order by kstr"
    sql "select connection_id() from fn_test"
    qt_sql "select conv(kbint, ktint, ktint) from fn_test order by kbint, ktint, ktint"
    qt_sql "select conv(kvchrs1, ktint, ktint) from fn_test order by kvchrs1, ktint, ktint"
    qt_sql "select conv(kstr, ktint, ktint) from fn_test order by kstr, ktint, ktint"
    qt_sql "select convert_to(kvchrs1, 'gbk') from fn_test order by kvchrs1, kvchrs1"
    qt_sql "select convert_tz(kdtm, 'Asia/Shanghai', 'Europe/Sofia') from fn_test order by kdtm, kvchrs1, kvchrs1"
    qt_sql "select convert_tz(kdtmv2s1, 'Asia/Shanghai', 'Europe/Sofia') from fn_test order by kdtmv2s1, kvchrs1, kvchrs1"
    qt_sql "select convert_tz(kdtv2, 'Asia/Shanghai', 'Europe/Sofia') from fn_test order by kdtv2, kvchrs1, kvchrs1"
    qt_sql "select cos(kdbl) from fn_test order by kdbl"
    sql "select current_date() from fn_test"
    sql "select current_time() from fn_test"
    sql "select current_timestamp() from fn_test"
    sql "select current_user() from fn_test"
    sql "select curtime() from fn_test"
    sql "select database() from fn_test"
    qt_sql "select date(kdtm) from fn_test order by kdtm"
    qt_sql "select date(kdtmv2s1) from fn_test order by kdtmv2s1"
    qt_sql "select datediff(kdtm, kdtm) from fn_test order by kdtm, kdtm"
    qt_sql "select datediff(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
    qt_sql "select datediff(kdtmv2s1, kdtv2) from fn_test order by kdtmv2s1, kdtv2"
    qt_sql "select datediff(kdtv2, kdtmv2s1) from fn_test order by kdtv2, kdtmv2s1"
    qt_sql "select datediff(kdtv2, kdtv2) from fn_test order by kdtv2, kdtv2"
    qt_sql "select datediff(kdtmv2s1, kdtm) from fn_test order by kdtmv2s1, kdtm"
    qt_sql "select datediff(kdtv2, kdtm) from fn_test order by kdtv2, kdtm"
    qt_sql "select date_format(kdtm, '2006-01-02 12:00:00') from fn_test order by kdtm"
    qt_sql "select date_format(kdt, '2006-01-02') from fn_test order by kdt"
    qt_sql "select date_format(kdtmv2s1, '2006-01-02 12:00:00') from fn_test order by kdtmv2s1"
    qt_sql "select date_format(kdtv2, '2006-01-02') from fn_test order by kdtv2"
    qt_sql "select date_trunc(kdtm, kvchrs1) from fn_test order by kdtm, kvchrs1"
    qt_sql "select date_trunc(kdtmv2s1, kvchrs1) from fn_test order by kdtmv2s1, kvchrs1"
    qt_sql "select datev2(kdtmv2s1) from fn_test order by kdtmv2s1"
    qt_sql "select day(kdtm) from fn_test order by kdtm"
    qt_sql "select day(kdtmv2s1) from fn_test order by kdtmv2s1"
    qt_sql "select day(kdtv2) from fn_test order by kdtv2"
    qt_sql "select day_ceil(kdtm) from fn_test order by kdtm"
    qt_sql "select day_ceil(kdtmv2s1) from fn_test order by kdtmv2s1"
    qt_sql "select day_ceil(kdtv2) from fn_test order by kdtv2"
    qt_sql "select day_ceil(kdtm, kdtm) from fn_test order by kdtm, kdtm"
    qt_sql "select day_ceil(kdtm, kint) from fn_test order by kdtm, kint"
    qt_sql "select day_ceil(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
    qt_sql "select day_ceil(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
    qt_sql "select day_ceil(kdtv2, kdtv2) from fn_test order by kdtv2, kdtv2"
    qt_sql "select day_ceil(kdtv2, kint) from fn_test order by kdtv2, kint"
    // core
    // qt_sql "select day_ceil(kdtm, kint, kdtm) from fn_test order by kdtm, kint, kdtm"
    qt_sql "select day_ceil(kdtmv2s1, kint, kdtmv2s1) from fn_test order by kdtmv2s1, kint, kdtmv2s1"
    qt_sql "select day_ceil(kdtv2, kint, kdtv2) from fn_test order by kdtv2, kint, kdtv2"
    qt_sql "select day_floor(kdtm) from fn_test order by kdtm"
    qt_sql "select day_floor(kdtmv2s1) from fn_test order by kdtmv2s1"
    qt_sql "select day_floor(kdtv2) from fn_test order by kdtv2"
    qt_sql "select day_floor(kdtm, kdtm) from fn_test order by kdtm, kdtm"
    qt_sql "select day_floor(kdtm, kint) from fn_test order by kdtm, kint"
    qt_sql "select day_floor(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
    qt_sql "select day_floor(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
    qt_sql "select day_floor(kdtv2, kdtv2) from fn_test order by kdtv2, kdtv2"
    qt_sql "select day_floor(kdtv2, kint) from fn_test order by kdtv2, kint"
    // core
    // qt_sql "select day_floor(kdtm, kint, kdtm) from fn_test order by kdtm, kint, kdtm"
    qt_sql "select day_floor(kdtmv2s1, kint, kdtmv2s1) from fn_test order by kdtmv2s1, kint, kdtmv2s1"
    qt_sql "select day_floor(kdtv2, kint, kdtv2) from fn_test order by kdtv2, kint, kdtv2"
    qt_sql "select dayname(kdtm) from fn_test order by kdtm"
    qt_sql "select dayname(kdtmv2s1) from fn_test order by kdtmv2s1"
    qt_sql "select dayname(kdtv2) from fn_test order by kdtv2"
    qt_sql "select dayofmonth(kdtm) from fn_test order by kdtm"
    qt_sql "select dayofmonth(kdtmv2s1) from fn_test order by kdtmv2s1"
    qt_sql "select dayofmonth(kdtv2) from fn_test order by kdtv2"
    qt_sql "select dayofweek(kdtm) from fn_test order by kdtm"
    qt_sql "select dayofweek(kdtmv2s1) from fn_test order by kdtmv2s1"
    qt_sql "select dayofweek(kdtv2) from fn_test order by kdtv2"
    qt_sql "select dayofyear(kdtm) from fn_test order by kdtm"
    qt_sql "select dayofyear(kdtmv2s1) from fn_test order by kdtmv2s1"
    qt_sql "select dayofyear(kdtv2) from fn_test order by kdtv2"
    qt_sql "select days_add(kdtm, kint) from fn_test order by kdtm, kint"
    qt_sql "select days_add(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
    qt_sql "select days_add(kdt, kint) from fn_test order by kdt, kint"
    qt_sql "select days_add(kdtv2, kint) from fn_test order by kdtv2, kint"
    qt_sql "select days_diff(kdtm, kdtm) from fn_test order by kdtm, kdtm"
    qt_sql "select days_diff(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
    qt_sql "select days_diff(kdtv2, kdtmv2s1) from fn_test order by kdtv2, kdtmv2s1"
    qt_sql "select days_diff(kdtmv2s1, kdtv2) from fn_test order by kdtmv2s1, kdtv2"
    qt_sql "select days_diff(kdtv2, kdtv2) from fn_test order by kdtv2, kdtv2"
    qt_sql "select days_diff(kdtv2, kdtm) from fn_test order by kdtv2, kdtm"
    qt_sql "select days_diff(kdtm, kdtv2) from fn_test order by kdtm, kdtv2"
    qt_sql "select days_diff(kdtmv2s1, kdtm) from fn_test order by kdtmv2s1, kdtm"
    qt_sql "select days_diff(kdtm, kdtmv2s1) from fn_test order by kdtm, kdtmv2s1"
    // cannot find function
    // qt_sql "select days_sub(kdtm, kint) from fn_test order by kdtm, kint"
    // qt_sql "select days_sub(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
    // qt_sql "select days_sub(kdt, kint) from fn_test order by kdt, kint"
    // qt_sql "select days_sub(kdtv2, kint) from fn_test order by kdtv2, kint"
    qt_sql "select dceil(kdbl) from fn_test order by kdbl"
    qt_sql "select degrees(kdbl) from fn_test order by kdbl"
    // data out of double range
    // qt_sql "select dexp(kdbl) from fn_test order by kdbl"
    qt_sql "select dfloor(kdbl) from fn_test order by kdbl"
    qt_sql "select digital_masking(kbint) from fn_test order by kbint"
    qt_sql "select dlog1(kdbl) from fn_test order by kdbl"
    qt_sql "select dlog10(kdbl) from fn_test order by kdbl"
    qt_sql "select domain(kstr) from fn_test order by kstr"
    qt_sql "select domain_without_www(kstr) from fn_test order by kstr"
    // data out of double range
    // qt_sql "select dpow(kdbl, kdbl) from fn_test order by kdbl, kdbl"
    // qt_sql "select dround(kdbl) from fn_test order by kdbl"
    // qt_sql "select dround(kdbl, kint) from fn_test order by kdbl, kint"
    // qt_sql "select dsqrt(kdbl) from fn_test order by kdbl"
    qt_sql "select e() from fn_test"
    // result error
    // qt_sql "select elt(kint, kvchrs1) from fn_test order by kint, kvchrs1"
    // qt_sql "select elt(kint, kstr) from fn_test order by kint, kstr"
    qt_sql "select ends_with(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
    qt_sql "select ends_with(kstr, kstr) from fn_test order by kstr, kstr"
    // cannot find function
    // qt_sql "select es_query(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
    // data out of range
    // qt_sql "select exp(kdbl) from fn_test order by kdbl"
    qt_sql "select extract_url_parameter(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
    qt_sql "select field(ktint, 1, 2) from fn_test order by ktint"
    qt_sql "select field(ksint, 1, 2) from fn_test order by ksint"
    qt_sql "select field(kint, 1, 2) from fn_test order by kint"
    qt_sql "select field(kbint, 1, 2) from fn_test order by kbint"
    qt_sql "select field(klint, 1, 2) from fn_test order by klint"
    qt_sql "select field(kfloat, 1, 2) from fn_test order by kfloat"
    qt_sql "select field(kdbl, 1, 2) from fn_test order by kdbl"
    qt_sql "select field(kdcmls1, 1, 2) from fn_test order by kdcmls1"
    qt_sql "select field(kdtv2, 1, 2) from fn_test order by kdtv2"
    qt_sql "select field(kdtmv2s1, 1, 2) from fn_test order by kdtmv2s1"
    qt_sql "select field(kvchrs1, 1, 2) from fn_test order by kvchrs1"
    qt_sql "select field(kstr, 1, 2) from fn_test order by kstr"
    qt_sql "select find_in_set(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
    qt_sql "select find_in_set(kstr, kstr) from fn_test order by kstr, kstr"
    qt_sql "select floor(kdbl) from fn_test order by kdbl"
    qt_sql "select fmod(kfloat, kfloat) from fn_test order by kfloat, kfloat"
    qt_sql "select fmod(kdbl, kdbl) from fn_test order by kdbl, kdbl"
    // data out of float range
    // qt_sql "select fpow(kdbl, kdbl) from fn_test order by kdbl, kdbl"
    sql "select from_base64(kvchrs1) from fn_test order by kvchrs1"
    sql "select from_base64(kstr) from fn_test order by kstr"
    qt_sql "select from_days(kint) from fn_test order by kint"
    qt_sql "select from_unixtime(kint) from fn_test order by kint"
    qt_sql "select from_unixtime(kint, 'varchar') from fn_test order by kint, kvchrs1"
    qt_sql "select from_unixtime(kint, 'string') from fn_test order by kint, kstr"
}