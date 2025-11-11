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

suite("nereids_scalar_fn_Array2") {
    sql 'use regression_test_nereids_function_p0'
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql ' set enable_decimal256=true; '

    order_qt_sql_array_sort_1 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END,
                                 [3, 2, null, 5, null, 1, 2])"""
    order_qt_sql_array_sort_2 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN 1
                                 WHEN y IS NULL THEN -1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END,
                                 [3, 2, null, 5, null, 1, 2])"""
    order_qt_sql_array_sort_3 """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), [3, 2, 5, 1, 2])"""
    order_qt_sql_array_sort_4 """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), ['bc', 'ab', 'dc'])"""
    order_qt_sql_array_sort_5 """SELECT array_sort((x, y) -> IF(length(x) < length(y), -1,
                               IF(length(x) = length(y), 0, 1)),
                               ['a', 'abcd', 'abc'])"""
    order_qt_sql_array_sort_6 """SELECT array_sort((x, y) -> IF(cardinality(x) < cardinality(y), -1,
                               IF(cardinality(x) = cardinality(y), 0, 1)),
                               [[2, 3, 1], [4, 2, 1, 4], [1, 2]])"""
    order_qt_sql_array_sort_7 """SELECT array_sort((x, y) -> IF(IPV4_STRING_TO_NUM_OR_NULL(x) < IPV4_STRING_TO_NUM_OR_NULL(y), -1,
                               IF(IPV4_STRING_TO_NUM_OR_NULL(x) = IPV4_STRING_TO_NUM_OR_NULL(y), 0, 1)),
                               ['192.168.0.3', '192.168.0.1', '192.168.0.2'])"""
    order_qt_sql_array_sort_8 """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), [3, -2.1, 5.34, 1.2, 2.2])"""

    order_qt_sql_array_sort_Tinyint """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(ktint) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Tinyint_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(ktint) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Smallint """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(ksint) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Smallint_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(ksint) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Int """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kint) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Int_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kint) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Bigint """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kbint) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Bigint_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kbint) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Largeint """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(klint) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_LargeInt_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(klint) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Float """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kfloat) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Float_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kfloat) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Double """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdbl) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Double_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdbl) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Demical1 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdcmls1) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Demical1_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdcmls1) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Demical2 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdcmls2) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Demical2_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdcmls2) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Demical3 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdcmls3) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Demical3_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdcmls3) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Demical4 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdcmlv3s1) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Demical4_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdcmlv3s1) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Demical5 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdcmlv3s2) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Demical5_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdcmlv3s2) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Demical6 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdcmlv3s3) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Demical6_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdcmlv3s3) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Char """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kchrs3) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Char_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kchrs3) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Varchar """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kvchrs3) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Varchar_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kvchrs3) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_String """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kstr) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_String_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kstr) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Date """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdt) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Date_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdt) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_DateV2 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdtv2) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_DateV2_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdtv2) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Datetime """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdtm) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Datetime_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdtm) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_DatetimeV2 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdtmv2s3) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_DatetimeV2_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdtmv2s3) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Boolean """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kbool) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Boolean_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kbool) AS arr FROM fn_test_not_nullable)t"""
}
