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

suite("test_snowflake_rewrite") {

    sql """ set enable_nereids_planner=true; """
    sql """ set enable_fallback_to_original_planner=false; """

    // =====================================================================
    // ZEROIFNULL: rewrites to COALESCE(x, 0)
    // =====================================================================
    qt_zeroifnull_with_value """ SELECT ZEROIFNULL(42) """
    qt_zeroifnull_with_null """ SELECT ZEROIFNULL(NULL) """
    qt_zeroifnull_zero """ SELECT ZEROIFNULL(0) """
    qt_zeroifnull_negative """ SELECT ZEROIFNULL(-5) """
    qt_zeroifnull_eq_coalesce """ SELECT ZEROIFNULL(NULL) = COALESCE(NULL, 0) """

    // ZEROIFNULL must reject non-numeric types
    test {
        sql """ SELECT ZEROIFNULL('hello') """
        exception "zeroifnull requires a numeric argument"
    }

    // =====================================================================
    // NULLIFZERO: rewrites to NULLIF(x, 0)
    // =====================================================================
    qt_nullifzero_zero """ SELECT NULLIFZERO(0) """
    qt_nullifzero_nonzero """ SELECT NULLIFZERO(42) """
    qt_nullifzero_null """ SELECT NULLIFZERO(NULL) """
    qt_nullifzero_negative """ SELECT NULLIFZERO(-1) """
    qt_nullifzero_eq_nullif """ SELECT NULLIFZERO(5) = NULLIF(5, 0) """

    // NULLIFZERO must reject non-numeric types
    test {
        sql """ SELECT NULLIFZERO('hello') """
        exception "nullifzero requires a numeric argument"
    }

    // =====================================================================
    // NVL2: rewrites to IF(NOT IS_NULL(e1), e2, e3)
    // =====================================================================
    qt_nvl2_not_null """ SELECT NVL2(1, 'present', 'absent') """
    qt_nvl2_null """ SELECT NVL2(NULL, 'present', 'absent') """
    qt_nvl2_zero """ SELECT NVL2(0, 'not null', 'is null') """
    qt_nvl2_empty_string """ SELECT NVL2('', 'not null', 'is null') """
    qt_nvl2_type_coercion """ SELECT NVL2(1, 100, 200) """
    qt_nvl2_null_type_coercion """ SELECT NVL2(NULL, 100, 200) """

    // =====================================================================
    // EQUAL_NULL: rewrites to NullSafeEqual (<=>)
    // Both NULL => true, one NULL => false
    // =====================================================================
    qt_equal_null_both_values """ SELECT EQUAL_NULL(1, 1) """
    qt_equal_null_diff_values """ SELECT EQUAL_NULL(1, 2) """
    qt_equal_null_both_null """ SELECT EQUAL_NULL(NULL, NULL) """
    qt_equal_null_left_null """ SELECT EQUAL_NULL(NULL, 1) """
    qt_equal_null_right_null """ SELECT EQUAL_NULL(1, NULL) """
    qt_equal_null_strings """ SELECT EQUAL_NULL('abc', 'abc') """
    qt_equal_null_diff_strings """ SELECT EQUAL_NULL('abc', 'xyz') """
    // Verify equivalence with <=>
    qt_equal_null_eq_nse_1 """ SELECT EQUAL_NULL(1, 1) = (1 <=> 1) """
    qt_equal_null_eq_nse_2 """ SELECT EQUAL_NULL(NULL, NULL) = (NULL <=> NULL) """

    // =====================================================================
    // CONVERT_TIMEZONE: rewrites to CONVERT_TZ with parameter reordering
    // Snowflake: CONVERT_TIMEZONE(src_tz, dst_tz, ts)
    // Doris:     CONVERT_TZ(ts, src_tz, dst_tz)
    // =====================================================================
    qt_convert_tz_basic """ SELECT CONVERT_TIMEZONE('UTC', 'America/New_York', '2024-01-15 12:00:00') """
    qt_convert_tz_same """ SELECT CONVERT_TIMEZONE('UTC', 'UTC', '2024-01-15 12:00:00') """
    // Verify equivalence with CONVERT_TZ (parameter reordering)
    qt_convert_tz_eq """ SELECT CONVERT_TIMEZONE('UTC', 'Asia/Shanghai', '2024-06-01 00:00:00') = CONVERT_TZ('2024-06-01 00:00:00', 'UTC', 'Asia/Shanghai') """

    // =====================================================================
    // OBJECT_CONSTRUCT V1: rewrites to JSON_OBJECT
    // =====================================================================
    qt_obj_construct_basic """ SELECT OBJECT_CONSTRUCT('name', 'Alice', 'age', 25) """
    qt_obj_construct_single """ SELECT OBJECT_CONSTRUCT('key', 'value') """
    qt_obj_construct_nested """ SELECT OBJECT_CONSTRUCT('a', 1, 'b', 'hello') """
    // Verify equivalence with JSON_OBJECT
    qt_obj_construct_eq_json """ SELECT OBJECT_CONSTRUCT('x', 1) = JSON_OBJECT('x', 1) """

    // =====================================================================
    // TO_VARCHAR / TO_CHAR: 1-arg rewrites to CAST, 2-arg rewrites to DATE_FORMAT
    // Snowflake supports both TO_VARCHAR and TO_CHAR as aliases.
    // =====================================================================
    // 1-arg: cast to VARCHAR
    qt_to_varchar_int """ SELECT TO_VARCHAR(12345) """
    qt_to_varchar_float """ SELECT TO_VARCHAR(3.14) """
    qt_to_varchar_string """ SELECT TO_VARCHAR('hello') """
    qt_to_varchar_null """ SELECT TO_VARCHAR(NULL) """
    // 2-arg: date format
    qt_to_varchar_date_fmt """ SELECT TO_VARCHAR(CAST('2024-01-15' AS DATE), '%Y-%m-%d') """
    qt_to_varchar_datetime_fmt """ SELECT TO_VARCHAR(CAST('2024-01-15 10:30:00' AS DATETIME), '%Y/%m/%d %H:%i:%s') """

    // TO_CHAR is a Snowflake alias for TO_VARCHAR
    qt_to_char_int """ SELECT TO_CHAR(12345) """
    qt_to_char_float """ SELECT TO_CHAR(3.14) """
    qt_to_char_date_fmt """ SELECT TO_CHAR(CAST('2024-01-15' AS DATE), '%Y-%m-%d') """
    // Verify TO_CHAR and TO_VARCHAR produce identical results
    qt_to_char_eq_to_varchar """ SELECT TO_CHAR(42) = TO_VARCHAR(42) """
}
